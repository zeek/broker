#include "broker/configuration.hh"

#include <ciso646>
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include <caf/config.hpp>
#include <caf/init_global_meta_objects.hpp>
#include <caf/io/middleman.hpp>
#include <caf/openssl/manager.hpp>

#include "broker/address.hh"
#include "broker/alm/lamport_timestamp.hh"
#include "broker/alm/multipath.hh"
#include "broker/config.hh"
#include "broker/core_actor.hh"
#include "broker/data.hh"
#include "broker/detail/retry_state.hh"
#include "broker/endpoint.hh"
#include "broker/internal_command.hh"
#include "broker/port.hh"
#include "broker/snapshot.hh"
#include "broker/status.hh"
#include "broker/store.hh"
#include "broker/subnet.hh"
#include "broker/time.hh"
#include "broker/topic.hh"
#include "broker/version.hh"

#ifdef BROKER_WINDOWS
#include <io.h>
#define STDOUT_FILENO 1
#define STDERR_FILENO 2
#define isatty _isatty
#else
#include <unistd.h>
#endif

namespace broker {

namespace {

constexpr const char* conf_file = "broker.conf";

template <class... Ts>
auto concat(Ts... xs) {
  std::string result;
  ((result += xs), ...);
  return result;
}

[[noreturn]] void throw_illegal_log_level(const char* var, const char* cstr) {
  auto what
    = concat("illegal value for environment variable ", var, ": '", cstr,
             "' (legal values: 'trace', 'debug', 'info', 'warning', 'error')");
  throw std::invalid_argument(what);
}

constexpr caf::string_view file_verbosity_key = "caf.logger.file.verbosity";

constexpr caf::string_view console_verbosity_key = "caf.logger.console.verbosity";

bool valid_log_level(caf::string_view x) {
  return x == "trace" || x == "debug" || x == "info" || x == "warning"
         || x == "error" || x == "quiet";
}

std::string to_log_level(const char* var, const char* cstr) {
  std::string str = cstr;
  if (valid_log_level(str))
    return str;
  throw_illegal_log_level(var, cstr);
}

} // namespace

configuration::configuration(skip_init_t) {
  // Add runtime type information for Broker types.
  init_global_state();
  add_message_types(*this);
  // Add custom options to the CAF parser.
  opt_group{custom_options_, "?broker"}
    .add(options_.disable_ssl, "disable-ssl",
         "forces Broker to use unencrypted communication")
    .add(options_.disable_forwarding, "disable-forwarding",
         "if true, turns the endpoint into a leaf node")
    .add<std::string>("recording-directory",
                      "path for storing recorded meta information")
    .add<size_t>("output-generator-file-cap",
                 "maximum number of entries when recording published messages")
    .add<size_t>("max-pending-inputs-per-source",
                 "maximum number of items we buffer per peer or publisher");
  sync_options();
  opt_group{custom_options_, "broker.store"}
    .add<caf::timespan>("tick-interval",
                        "time interval for advancing the local Lamport time")
    .add<uint16_t>("heartbeat-interval",
                   "number of ticks between heartbeat messages")
    .add<uint16_t>("nack-timeout",
                   "number of ticks before sending NACK messages")
    .add<uint16_t>("connection-timeout",
                   "number of heartbeats a remote store is allowed to miss");
  // Ensure that we're only talking to compatible Broker instances.
  std::vector<std::string> ids{"broker.v" + std::to_string(version::protocol)};
  // Override CAF defaults.
  set("caf.logger.file.path", "broker_[PID]_[TIMESTAMP].log");
  set("caf.logger.file.verbosity", "quiet");
  set("caf.logger.console.format", "[%c/%p] %d %m");
  set("caf.logger.console.verbosity", "error");
  // Broker didn't load the MM module yet. Use `put` to suppress the 'failed to
  // set config parameter' warning on the command line.
  put(content, "caf.middleman.app-identifiers", std::move(ids));
  put(content, "caf.middleman.workers", 0);
  // Turn off all CAF output by default.
  std::vector<std::string> excluded_components{"caf", "caf_io", "caf_net",
                                               "caf_flow", "caf_stream"};
  set("caf.logger.file.excluded-components", excluded_components);
  set("caf.logger.console.excluded-components", std::move(excluded_components));
}

configuration::configuration(broker_options opts) : configuration(skip_init) {
  options_ = opts;
  sync_options();
  init(0, nullptr);
}

configuration::configuration() : configuration(skip_init) {
  init(0, nullptr);
}

configuration::configuration(int argc, char** argv) : configuration(skip_init) {
  init(argc, argv);
}

void configuration::init(int argc, char** argv) {
  std::vector<std::string> args;
  if (argc > 1 && argv != nullptr)
    args.assign(argv + 1, argv + argc);
  // Load CAF modules.
  load<caf::io::middleman>();
  if (not options_.disable_ssl)
    load<caf::openssl::manager>();
  // Phase 1: parse broker.conf or configuration file specified by the user on
  //          the command line (overrides hard-coded defaults).
  if (!options_.ignore_broker_conf) {
    std::vector<std::string> args_subset;
    auto predicate = [](const std::string& str) {
      return str.compare(0, 14, "--config-file=") != 0;
    };
    auto sep = std::stable_partition(args.begin(), args.end(), predicate);
    if(sep != args.end()) {
      args_subset.assign(std::make_move_iterator(sep),
                         std::make_move_iterator(args.end()));
      args.erase(sep, args.end());
    }
    if (auto err = parse(std::move(args_subset), conf_file)) {
      auto what = concat("Error while reading ", conf_file, ": ",
                         to_string(err));
      throw std::runtime_error(what);
    }
  }
  // Phase 2: parse environment variables (override config file settings).
  if (auto console_verbosity = getenv("BROKER_CONSOLE_VERBOSITY")) {
    auto level = to_log_level("BROKER_CONSOLE_VERBOSITY", console_verbosity);
    set(console_verbosity_key, level);
  }
  if (auto file_verbosity = getenv("BROKER_FILE_VERBOSITY")) {
    auto level = to_log_level("BROKER_FILE_VERBOSITY", file_verbosity);
    set(file_verbosity_key, level);
  }
  if (auto env = getenv("BROKER_RECORDING_DIRECTORY")) {
    set("broker.recording-directory", env);
  }
  if (auto env = getenv("BROKER_OUTPUT_GENERATOR_FILE_CAP")) {
    char* end = nullptr;
    auto value = strtol(env, &end, 10);
    if (errno == ERANGE || *end != '\0' || value < 0) {
      auto what
        = concat("invalid value for BROKER_OUTPUT_GENERATOR_FILE_CAP: ", env,
                 " (expected a positive integer)");
      throw std::invalid_argument(what);
    }
    set("broker.output-generator-file-cap", static_cast<size_t>(value));
  }
  // Phase 3: parse command line arguments.
  if (!args.empty()) {
    std::stringstream dummy;
    if (auto err = parse(std::move(args), dummy)) {
      auto what = concat("Error while parsing CLI arguments: ", to_string(err));
      throw std::runtime_error(what);
    }
  }
}

caf::settings configuration::dump_content() const {
  auto result = super::dump_content();
  auto& grp = result["broker"].as_dictionary();
  put_missing(grp, "disable-ssl", options_.disable_ssl);
  put_missing(grp, "disable-forwarding", options_.disable_forwarding);
  if (auto path = get_if<std::string>(&content, "broker.recording-directory"))
    put_missing(grp, "recording-directory", *path);
  if (auto cap = get_if<size_t>(&content, "broker.output-generator-file-cap"))
    put_missing(grp, "output-generator-file-cap", *cap);
  namespace pb = broker::defaults::path_revocations;
  auto& sub_grp = grp["path-revocations"].as_dictionary();
  put_missing(sub_grp, "aging-interval", pb::aging_interval);
  put_missing(sub_grp, "max-age", pb::max_age);
  return result;
}

void configuration::add_message_types(caf::actor_system_config&) {
  // nop
}

namespace {

std::once_flag init_global_state_flag;

} // namespace

void configuration::init_global_state() {
  std::call_once(init_global_state_flag, [] {
    caf::init_global_meta_objects<caf::id_block::broker>();
    caf::openssl::manager::init_global_meta_objects();
    caf::io::middleman::init_global_meta_objects();
    caf::core::init_global_meta_objects();
  });
}

void configuration::sync_options() {
  set("broker.disable-ssl", options_.disable_ssl);
  set("broker.disable-forwarding", options_.disable_forwarding);
}

} // namespace broker
