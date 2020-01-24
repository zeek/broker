#include "broker/configuration.hh"

#include <cstdlib>
#include <cstring>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include <caf/atom.hpp>
#include <caf/io/middleman.hpp>
#include <caf/openssl/manager.hpp>

#include "broker/address.hh"
#include "broker/data.hh"
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

#include <unistd.h>

namespace broker {

namespace {

constexpr const char* conf_file = "broker.conf";

template <class... Ts>
auto concat(Ts... xs) {
  std::string result;
  ((result += xs), ...);
  return result;
}

bool valid_log_level(caf::atom_value x) {
  using caf::atom_uint;
  switch (atom_uint(x)) {
    default:
      return false;
    case atom_uint("trace"):
    case atom_uint("debug"):
    case atom_uint("info"):
    case atom_uint("warning"):
    case atom_uint("error"):
    case atom_uint("quiet"):
      return true;
  }
}

optional<caf::atom_value> to_log_level(const char* cstr) {
  caf::string_view str{cstr, strlen(cstr)};
  auto atm = caf::to_lowercase(caf::atom_from_string(str));
  if (valid_log_level(atm))
    return atm;
  return nil;
}

[[noreturn]] void throw_illegal_log_level(const char* var, const char* cstr) {
  auto what
    = concat("illegal value for environment variable ", var, ": '", cstr,
             "' (legal values: 'trace', 'debug', 'info', 'warning', 'error')");
  throw std::invalid_argument(what);
}

} // namespace

configuration::configuration(skip_init_t) {
  // Add runtime type information for Broker types.
  add_message_types(*this);
  // Ensure that we're only talking to compatible Broker instances.
  std::vector<std::string> ids{"broker.v" + std::to_string(version::protocol)};
  set("middleman.app-identifiers", std::move(ids));
  // Add custom options to the CAF parser.
  opt_group{custom_options_, "?broker"}
    .add(options_.disable_ssl, "disable_ssl",
         "forces Broker to use unencrypted communication")
    .add(options_.ttl, "ttl", "drop messages after traversing TTL hops")
    .add<std::string>("recording-directory",
                      "path for storing recorded meta information")
    .add<size_t>("output-generator-file-cap",
                 "maximum number of entries when recording published messages");
  // Override CAF defaults.
  using caf::atom;
  set("logger.file-name", "broker_[PID]_[TIMESTAMP].log");
  set("logger.file-verbosity", atom("quiet"));
  set("logger.console-format", "[%c/%p] %d %m");
  // Enable console output (and color it if stdout is a TTY) but set verbosty to
  // quiet. This allows users to only care about the environment variable
  // BROKER_CONSOLE_VERBOSITY.
  if (isatty(STDOUT_FILENO))
    set("logger.console", atom("colored"));
  else
    set("logger.console", atom("uncolored"));
  set("logger.console-verbosity", atom("quiet"));
  // Turn off all CAF output by default.
  std::vector<caf::atom_value> blacklist{atom("caf"), atom("caf_io"),
                                         atom("caf_net"), atom("caf_flow"),
                                         atom("caf_stream")};
  set("logger.component-blacklist", std::move(blacklist));
}


configuration::configuration(broker_options opts) : configuration(skip_init) {
  options_ = opts;
  init(0, nullptr);
}

configuration::configuration() : configuration(skip_init) {
  init(0, nullptr);
}

configuration::configuration(int argc, char** argv) : configuration(skip_init) {
  init(argc, argv);
}

void configuration::init(int argc, char** argv) {
  // Load CAF modules.
  load<caf::io::middleman>();
  if (not options_.disable_ssl)
    load<caf::openssl::manager>();
  // Phase 1: parse broker.conf (overrides hard-coded defaults).
  if (!options_.ignore_broker_conf) {
    if (auto err = parse(0, nullptr, conf_file)) {
      auto what = concat("Error while reading ", conf_file, ": ", render(err));
      throw std::runtime_error(what);
    }
  }
  // Phase 2: parse environment variables (override config file settings).
  if (auto console_verbosity = getenv("BROKER_CONSOLE_VERBOSITY")) {
    if (auto level = to_log_level(console_verbosity))
      set("logger.console-verbosity", *level);
    else
      throw_illegal_log_level("BROKER_CONSOLE_VERBOSITY", console_verbosity);
  }
  if (auto file_verbosity = getenv("BROKER_FILE_VERBOSITY")) {
    if (auto level = to_log_level(file_verbosity))
      set("logger.file-verbosity", *level);
    else
      throw_illegal_log_level("BROKER_FILE_VERBOSITY", file_verbosity);
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
  if (argc == 0 || argv == nullptr)
    return;
  std::stringstream dummy;
  if (auto err = parse(argc, argv, dummy)) {
    auto what = concat("Error while parsing CLI arguments: ", render(err));
    throw std::runtime_error(what);
  }
}

caf::settings configuration::dump_content() const {
  auto result = super::dump_content();
  auto& grp = result["broker"].as_dictionary();
  put_missing(grp, "disable_ssl", options_.disable_ssl);
  put_missing(grp, "ttl", options_.ttl);
  put_missing(grp, "forward", options_.forward);
  if (auto path = get_if<std::string>(&content, "broker.recording-directory"))
    put_missing(grp, "recording-directory", *path);
  if (auto cap = get_if<size_t>(&content, "broker.output-generator-file-cap"))
    put_missing(grp, "output-generator-file-cap", *cap);
  return result;
}

#define ADD_MSG_TYPE(name) cfg.add_message_type<name>(#name)

void configuration::add_message_types(caf::actor_system_config& cfg) {
  ADD_MSG_TYPE(broker::data);
  ADD_MSG_TYPE(broker::address);
  ADD_MSG_TYPE(broker::subnet);
  ADD_MSG_TYPE(broker::port);
  ADD_MSG_TYPE(broker::timespan);
  ADD_MSG_TYPE(broker::timestamp);
  ADD_MSG_TYPE(broker::enum_value);
  ADD_MSG_TYPE(broker::vector);
  ADD_MSG_TYPE(broker::set);
  ADD_MSG_TYPE(broker::status);
  ADD_MSG_TYPE(broker::table);
  ADD_MSG_TYPE(broker::topic);
  ADD_MSG_TYPE(broker::optional<broker::timestamp>);
  ADD_MSG_TYPE(broker::optional<broker::timespan>);
  ADD_MSG_TYPE(broker::snapshot);
  ADD_MSG_TYPE(broker::internal_command);
  ADD_MSG_TYPE(broker::command_message);
  ADD_MSG_TYPE(broker::data_message);
  ADD_MSG_TYPE(broker::node_message);
  ADD_MSG_TYPE(broker::node_message::value_type);
  ADD_MSG_TYPE(broker::set_command);
  ADD_MSG_TYPE(broker::store::stream_type::value_type);
}

#undef ADD_MSG_TYPE

} // namespace broker
