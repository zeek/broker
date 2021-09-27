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
#include <caf/net/middleman.hpp>

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

std::vector<std::string> split_and_trim(const char* str, char delim = ',') {
  auto trim = [](std::string& x) {
    auto predicate = [](int ch) { return !std::isspace(ch); };
    x.erase(x.begin(), std::find_if(x.begin(), x.end(), predicate));
    x.erase(std::find_if(x.rbegin(), x.rend(), predicate).base(), x.end());
  };
  auto is_empty = [](const std::string& x) { return x.empty(); };
  std::vector<std::string> result;
  caf::split(result, caf::string_view{str, strlen(str)}, delim,
             caf::token_compress_on);
  std::for_each(result.begin(), result.end(), trim);
  result.erase(std::remove_if(result.begin(), result.end(), is_empty),
               result.end());
  return result;
}

} // namespace

configuration::configuration(skip_init_t) {
  using std::string;
  using string_list = std::vector<string>;
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
                 "maximum number of items we buffer per peer or publisher")
    .add<bool>("disable-connector",
               "run without a connector (primarily only for testing)");
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
  opt_group{custom_options_, "broker.metrics"}
    .add<uint16_t>("port", "port for incoming Prometheus (HTTP) requests")
    .add<string>("address", "bind address for the HTTP server socket")
    .add<string>("endpoint-name",
                 "name for this endpoint in metrics (when exporting: suffix of "
                 "the topic by default)");
  opt_group{custom_options_, "broker.metrics.export"}
    .add<string>("topic",
                 "if set, causes Broker to publish its metrics "
                 "periodically on the given topic")
    .add<caf::timespan>("interval",
                        "time between publishing metrics on the topic")
    .add<string_list>("prefixes",
                      "selects metric prefixes to publish on the topic");
  opt_group{custom_options_, "broker.metrics.import"} //
    .add<string_list>("topics", "topics for collecting remote metrics from");
  // Ensure that we're only talking to compatible Broker instances.
  string_list ids{"broker.v" + std::to_string(version::protocol)};
  // Override CAF defaults.
  set("caf.logger.file.path", "broker_[PID]_[TIMESTAMP].log");
  set("caf.logger.file.verbosity", "quiet");
  set("caf.logger.console.format", "[%c/%p] %d %m");
  set("caf.logger.console.verbosity", "error");
  // Turn off all CAF output by default.
  // string_list excluded_components{"caf", "caf_io", "caf_net", "caf_flow",
  //                                 "caf_stream"};
  // set("caf.logger.file.excluded-components", excluded_components);
  // set("caf.logger.console.excluded-components", std::move(excluded_components));
}

configuration::configuration(broker_options opts) : configuration(skip_init) {
  options_ = opts;
  sync_options();
  init(0, nullptr);
  config_file_path = "broker.conf";
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
  load<caf::net::middleman>();
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
    if (auto err = parse(std::move(args_subset))) {
      auto what = concat("Error while reading configuration file: ",
                         to_string(err));
      throw std::runtime_error(what);
    }
  }
  put(content, "caf.metrics-filters.actors.includes",
      std::vector<std::string>{core_state::name});
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
  if (auto env = getenv("BROKER_METRICS_PORT")) {
    caf::config_value val{env};
    if (auto port = caf::get_as<uint16_t>(val)) {
      set("broker.metrics.port", *port);
    } else {
      auto what = concat("invalid value for BROKER_METRICS_PORT: ", env,
                         " (expected a non-zero port number)");
      throw std::invalid_argument(what);
    }
  }
  if (auto env = getenv("BROKER_METRICS_ENDPOINT_NAME")) {
    set("broker.metrics.endpoint-name", env);
  }
  if (auto env = getenv("BROKER_METRICS_EXPORT_TOPIC")) {
    set("broker.metrics.export.topic", env);
  }
  if (auto env = getenv("BROKER_METRICS_EXPORT_INTERVAL")) {
    caf::config_value val{env};
    if (auto interval = caf::get_as<caf::timespan>(val)) {
      set("broker.metrics.export.interval", *interval);
    } else {
      auto what = concat("invalid value for BROKER_METRICS_EXPORT_INTERVAL: ",
                         env, " (expected an interval such as '5s')");
      throw std::invalid_argument(what);
    }
  }
  if (auto env = getenv("BROKER_METRICS_EXPORT_PREFIXES")) {
    if (auto prefixes = split_and_trim(env); !prefixes.empty())
      set("broker.metrics.export.prefixes", std::move(prefixes));
  }
  if (auto env = getenv("BROKER_METRICS_IMPORT_TOPICS")) {
    if (auto topics = split_and_trim(env); !topics.empty())
      set("broker.metrics.import.topics", std::move(topics));
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
  if (auto path = get_as<std::string>(content, "broker.recording-directory"))
    put_missing(grp, "recording-directory", *path);
  if (auto cap = get_as<size_t>(content, "broker.output-generator-file-cap"))
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
    caf::io::middleman::init_global_meta_objects();
    caf::core::init_global_meta_objects();
  });
}

void configuration::sync_options() {
  set("broker.disable-ssl", options_.disable_ssl);
  set("broker.disable-forwarding", options_.disable_forwarding);
}

} // namespace broker
