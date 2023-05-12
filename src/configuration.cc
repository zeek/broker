#include "broker/configuration.hh"

#include <ciso646>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include <caf/actor_system_config.hpp>
#include <caf/config.hpp>
#include <caf/init_global_meta_objects.hpp>
#include <caf/io/middleman.hpp>
#include <caf/net/middleman.hpp>

#include "broker/address.hh"
#include "broker/alm/multipath.hh"
#include "broker/config.hh"
#include "broker/data.hh"
#include "broker/endpoint.hh"
#include "broker/internal/configuration_access.hh"
#include "broker/internal/core_actor.hh"
#include "broker/internal/native.hh"
#include "broker/internal/retry_state.hh"
#include "broker/internal/type_id.hh"
#include "broker/internal_command.hh"
#include "broker/lamport_timestamp.hh"
#include "broker/port.hh"
#include "broker/snapshot.hh"
#include "broker/status.hh"
#include "broker/store.hh"
#include "broker/subnet.hh"
#include "broker/time.hh"
#include "broker/topic.hh"
#include "broker/version.hh"

#ifdef BROKER_WINDOWS
#  include <io.h>
#  define STDOUT_FILENO 1
#  define STDERR_FILENO 2
#  define isatty _isatty
#else
#  include <unistd.h>
#endif

namespace broker {

bool openssl_options::authentication_enabled() const noexcept {
  return !certificate.empty() || !key.empty() || !passphrase.empty()
         || !capath.empty() || !cafile.empty();
}

namespace {

template <class... Ts>
auto concat(Ts... xs) {
  std::string result;
  ((result += xs), ...);
  return result;
}

[[noreturn]] void throw_illegal_log_level(const char* var, const char* cstr) {
  auto what =
    concat("illegal value for environment variable ", var, ": '", cstr,
           "' (legal values: 'trace', 'debug', 'info', 'warning', 'error')");
  throw std::invalid_argument(what);
}

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

struct configuration::impl : public caf::actor_system_config {
  using super = caf::actor_system_config;

  impl() : ssl_options(std::make_shared<broker::openssl_options>()) {
    using std::string;
    using string_list = std::vector<string>;
    // Add custom options to the CAF parser.
    opt_group{custom_options_, "?broker"}
      .add(options.disable_ssl, "disable-ssl",
           "forces Broker to use unencrypted communication")
      .add(options.disable_forwarding, "disable-forwarding",
           "disables forwarding of incoming data to peers")
      .add(options.ttl, "ttl", "drop messages after traversing TTL hops")
      .add<string>("recording-directory",
                   "path for storing recorded meta information")
      .add<size_t>(
        "output-generator-file-cap",
        "maximum number of entries when recording published messages")
      .add<size_t>("max-pending-inputs-per-source",
                   "maximum number of items we buffer per peer or publisher");
    opt_group{custom_options_, "broker.web-socket"} //
      .add<string>("address", "bind address for the WebSocket server socket")
      .add<port>("port", "port for incoming WebSocket connections");
    opt_group{custom_options_, "broker.metrics"}
      .add<port>("port", "port for incoming Prometheus (HTTP) requests")
      .add<string>("address", "bind address for the HTTP server socket")
      .add<string>(
        "endpoint-name",
        "name for this endpoint in metrics (when exporting: suffix of "
        "the topic by default)");
    opt_group{custom_options_, "broker.metrics.export"}
      .add<string>("topic", "if set, causes Broker to publish its metrics "
                            "periodically on the given topic")
      .add<caf::timespan>("interval",
                          "time between publishing metrics on the topic")
      .add<string_list>("prefixes",
                        "selects metric prefixes to publish on the topic");
    opt_group{custom_options_, "broker.metrics.import"} //
      .add<string_list>("topics", "topics for collecting remote metrics from");
    opt_group{custom_options_, "broker.ssl"} //
      .add(ssl_options->certificate, "certificate",
           "path to the PEM-formatted certificate file")
      .add(ssl_options->key, "key",
           "path to the private key file for this node")
      .add(ssl_options->passphrase, "passphrase",
           "passphrase to decrypt the private key")
      .add(ssl_options->capath, "capath",
           "path to an OpenSSL-style directory of trusted certificates")
      .add(ssl_options->cafile, "cafile",
           "path to a file of concatenated PEM-formatted certificates");
    // Ensure that we're only talking to compatible Broker instances.
    string_list ids{"broker.v" + std::to_string(version::protocol)};
    // Override CAF defaults.
    set("caf.logger.file.path", "broker_[PID]_[TIMESTAMP].log");
    set("caf.logger.file.verbosity", "quiet");
    set("caf.logger.console.format", "[%c/%p] %d %m");
    set("caf.logger.console.verbosity", "error");
    // Broker didn't load the MM module yet. Use `put` to suppress the 'failed
    // to set config parameter' warning on the command line.
    put(content, "caf.middleman.app-identifiers", std::move(ids));
    put(content, "caf.middleman.workers", 0);
    // Turn off all CAF output by default.
    string_list excluded_components{"caf", "caf_io", "caf_net", "caf_flow",
                                    "caf_stream"};
    set("caf.logger.file.excluded-components", excluded_components);
    set("caf.logger.console.excluded-components",
        std::move(excluded_components));
  }

  caf::settings dump_content() const override {
    auto result = super::dump_content();
    auto& grp = result["broker"].as_dictionary();
    put_missing(grp, "disable-ssl", options.disable_ssl);
    put_missing(grp, "ttl", options.ttl);
    put_missing(grp, "disable-forwarding", options.disable_forwarding);
    if (auto path = get_as<std::string>(content, "broker.recording-directory"))
      put_missing(grp, "recording-directory", std::move(*path));
    if (auto cap = get_as<size_t>(content, "broker.output-generator-file-cap"))
      put_missing(grp, "output-generator-file-cap", *cap);
    return result;
  }

  void init(int argc, char** argv);

  broker_options options;

  openssl_options_ptr ssl_options;
};

configuration::configuration(skip_init_t) {
  using std::string;
  using string_list = std::vector<string>;
  init_global_state();
  impl_ = std::make_unique<impl>();
}

configuration::configuration(broker_options opts) : configuration(skip_init) {
  impl_->options = opts;
  impl_->set("broker.ttl", opts.ttl);
  caf::put(impl_->content, "disable-forwarding", opts.disable_forwarding);
  init(0, nullptr);
  impl_->config_file_path = "broker.conf";
}

configuration::configuration() : configuration(skip_init) {
  init(0, nullptr);
}

configuration::configuration(configuration&& other) noexcept
  : impl_(std::move(other.impl_)) {
  // cannot '= default' this because impl is incomplete in the header.
}

configuration::configuration(int argc, char** argv) : configuration(skip_init) {
  init(argc, argv);
}

configuration::~configuration() {
  // nop, but must stay out-of-line because impl is incomplete in the header.
}

void configuration::impl::init(int argc, char** argv) {
  std::vector<std::string> args;
  if (argc > 1 && argv != nullptr)
    args.assign(argv + 1, argv + argc);
  // Load CAF modules.
  load<caf::net::middleman>();
  // Phase 1: parse broker.conf or configuration file specified by the user on
  //          the command line (overrides hard-coded defaults).
  if (!options.ignore_broker_conf) {
    std::vector<std::string> args_subset;
    auto predicate = [](const std::string& str) {
      return str.compare(0, 14, "--config-file=") != 0;
    };
    auto sep = std::stable_partition(args.begin(), args.end(), predicate);
    if (sep != args.end()) {
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
      std::vector<std::string>{internal::core_actor_state::name});
  // Phase 2: parse environment variables (override config file settings).
  if (auto console_verbosity = getenv("BROKER_CONSOLE_VERBOSITY")) {
    auto level = to_log_level("BROKER_CONSOLE_VERBOSITY", console_verbosity);
    set("caf.logger.console.verbosity", level);
  }
  if (auto file_verbosity = getenv("BROKER_FILE_VERBOSITY")) {
    auto level = to_log_level("BROKER_FILE_VERBOSITY", file_verbosity);
    set("caf.logger.file.verbosity", level);
  }
  if (auto env = getenv("BROKER_RECORDING_DIRECTORY")) {
    set("broker.recording-directory", env);
  }
  if (auto env = getenv("BROKER_WEB_SOCKET_PORT")) {
    // Check for validity before overriding any CLI or config file value.
    auto str = std::string{env};
    broker::port tmp;
    if (!convert(str, tmp)) {
      auto what = concat("invalid value for BROKER_WEB_SOCKET_PORT: ", env,
                         " (expected a non-zero port number)");
      throw std::invalid_argument(what);
    }
    set("broker.web-socket.port", std::move(str));
  }
  if (auto env = getenv("BROKER_METRICS_PORT")) {
    // Check for validity before overriding any CLI or config file value.
    auto str = std::string{env};
    broker::port tmp;
    if (!convert(str, tmp)) {
      auto what = concat("invalid value for BROKER_METRICS_PORT: ", env,
                         " (expected a non-zero port number)");
      throw std::invalid_argument(what);
    }
    set("broker.metrics.port", std::move(str));
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
      auto what = concat("invalid value for BROKER_OUTPUT_GENERATOR_FILE_CAP: ",
                         env, " (expected a positive integer)");
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

void configuration::init(int argc, char** argv) {
  impl_->init(argc, argv);
}

const broker_options& configuration::options() const {
  return impl_->options;
}

std::string configuration::help_text() const {
  return impl_->custom_options().help_text();
}

const std::vector<std::string>& configuration::remainder() const {
  return impl_->remainder;
}

bool configuration::cli_helptext_printed() const {
  return impl_->cli_helptext_printed;
}

std::string configuration::openssl_certificate() const {
  return impl_->ssl_options->certificate;
}

void configuration::openssl_certificate(std::string x) {
  impl_->ssl_options->certificate = std::move(x);
}

std::string configuration::openssl_key() const {
  return impl_->ssl_options->key;
}

void configuration::openssl_key(std::string x) {
  impl_->ssl_options->key = std::move(x);
}

std::string configuration::openssl_passphrase() const {
  return impl_->ssl_options->passphrase;
}

void configuration::openssl_passphrase(std::string x) {
  impl_->ssl_options->passphrase = std::move(x);
}

std::string configuration::openssl_capath() const {
  return impl_->ssl_options->capath;
}

void configuration::openssl_capath(std::string x) {
  impl_->ssl_options->capath = std::move(x);
}

std::string configuration::openssl_cafile() const {
  return impl_->ssl_options->cafile;
}

void configuration::openssl_cafile(std::string x) {
  impl_->ssl_options->cafile = std::move(x);
}

openssl_options_ptr configuration::openssl_options() const {
  if (!options().disable_ssl) {
    return impl_->ssl_options;
  } else {
    return nullptr;
  }
}

void configuration::add_option(int64_t* dst, std::string_view name,
                               std::string_view description) {
  if (dst)
    impl_->custom_options().add(*dst, "global", name, description);
  else
    impl_->custom_options().add<int64_t>("global", name, description);
}

void configuration::add_option(uint64_t* dst, std::string_view name,
                               std::string_view description) {
  if (dst)
    impl_->custom_options().add(*dst, "global", name, description);
  else
    impl_->custom_options().add<uint64_t>("global", name, description);
}

void configuration::add_option(double* dst, std::string_view name,
                               std::string_view description) {
  if (dst)
    impl_->custom_options().add(*dst, "global", name, description);
  else
    impl_->custom_options().add<double>("global", name, description);
}

void configuration::add_option(bool* dst, std::string_view name,
                               std::string_view description) {
  if (dst)
    impl_->custom_options().add(*dst, "global", name, description);
  else
    impl_->custom_options().add<bool>("global", name, description);
}

void configuration::add_option(std::string* dst, std::string_view name,
                               std::string_view description) {
  if (dst)
    impl_->custom_options().add(*dst, "global", name, description);
  else
    impl_->custom_options().add<std::string>("global", name, description);
}

void configuration::add_option(std::vector<std::string>* dst,
                               std::string_view name,
                               std::string_view description) {
  if (dst)
    impl_->custom_options().add(*dst, "global", name, description);
  else
    impl_->custom_options().add<std::vector<std::string>>("global", name,
                                                          description);
}

void configuration::set(std::string_view key, timespan val) {
  impl_->set(key, val);
}

void configuration::set(std::string_view key, std::string val) {
  impl_->set(key, std::move(val));
}

void configuration::set(std::string_view key, std::vector<std::string> val) {
  impl_->set(key, std::move(val));
}

void configuration::set_i64(std::string_view key, int64_t val) {
  impl_->set(key, val);
}

void configuration::set_u64(std::string_view key, uint64_t val) {
  impl_->set(key, val);
}

void configuration::set_bool(std::string_view key, bool val) {
  impl_->set(key, val);
}

std::optional<int64_t> configuration::read_i64(std::string_view key,
                                               int64_t min_val,
                                               int64_t max_val) const {
  // Read the value as an int64_t and perform a range check. On success, return
  // the value.
  if (auto res = caf::get_as<int64_t>(*impl_, key);
      res && *res >= min_val && *res <= max_val)
    return {*res};
  // Special case: if the value is a port, we allow a conversion here.
  if (auto res = caf::get_as<port>(*impl_, key);
      res && res->number() >= min_val && res->number() <= max_val)
    return {static_cast<int64_t>(res->number())};
  // No matching conversion: return nullopt.
  return {};
}

std::optional<uint64_t> configuration::read_u64(std::string_view key,
                                                uint64_t max_val) const {
  // Read the value as an uint64_t and perform a range check. On success, return
  // the value.
  if (auto res = caf::get_as<uint64_t>(*impl_, key); res && *res <= max_val)
    return {*res};
  // Special case: if the value is a port, we allow a conversion here.
  if (auto res = caf::get_as<port>(*impl_, key);
      res && res->number() <= max_val)
    return {static_cast<uint64_t>(res->number())};
  // No matching conversion: return nullopt.
  return {};
}

std::optional<timespan> configuration::read_ts(std::string_view key) const {
  if (auto res = caf::get_as<caf::timespan>(*impl_, key))
    return {*res};
  else
    return {};
}

std::optional<std::string> configuration::read_str(std::string_view key) const {
  if (auto res = caf::get_as<std::string>(*impl_, key))
    return {std::move(*res)};
  else
    return {};
}

std::optional<std::vector<std::string>>
configuration::read_str_vec(std::string_view key) const {
  if (auto res = caf::get_as<std::vector<std::string>>(*impl_, key))
    return {std::move(*res)};
  else
    return {};
}

namespace {

std::once_flag init_global_state_flag;

} // namespace

void configuration::init_global_state() {
  std::call_once(init_global_state_flag, [] {
    caf::init_global_meta_objects<caf::id_block::broker_internal>();
    caf::io::middleman::init_global_meta_objects();
    caf::core::init_global_meta_objects();
  });
}

} // namespace broker

namespace broker::internal {

caf::actor_system_config& configuration_access::cfg() {
  return *ptr->impl_;
}

} // namespace broker::internal
