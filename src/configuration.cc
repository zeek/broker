#include "broker/configuration.hh"

#include <cstdlib>
#include <cstring>
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

namespace broker {

configuration::configuration(broker_options opts) : options_(std::move(opts)) {
  // Add runtime type information for Broker types.
  add_message_types(*this);
  // Load CAF modules.
  load<caf::io::middleman>();
  if (not options_.disable_ssl)
    load<caf::openssl::manager>();
  // Ensure that we're only talking to compatible Broker instances.
  set("middleman.app-identifier",
      "broker.v" + std::to_string(version::protocol));
  // Add custom options to the CAF parser.
  opt_group{custom_options_, "?broker"}
    .add(options_.disable_ssl, "disable_ssl",
         "forces Broker to use unencrypted communication")
    .add(options_.ttl, "ttl", "drop messages after traversing TTL hops")
    .add<std::string>("output-generator-file",
                      "records meta information for each published message")
    .add<size_t>("output-generator-file-cap",
                 "maximum number of entries when recording published messages");
  // Override CAF default file names.
  set("logger.file-name", "broker_[PID]_[TIMESTAMP].log");
  // Check for supported environment variables.
  if (auto env = getenv("BROKER_DEBUG_VERBOSE")) {
    if (*env && *env != '0') {
      set("logger.verbosity", caf::atom("DEBUG"));
      set("logger.component-filter", "");
    }
  }
  if (auto env = getenv("BROKER_DEBUG_LEVEL")) {
    char level[10];
    strncpy(level, env, sizeof(level));
    level[sizeof(level) - 1] = '\0';
    set("logger.verbosity", caf::atom(level));
  }
  if (auto env = getenv("BROKER_DEBUG_COMPONENT_FILTER"))
    set("logger.component-filter", env);
  if (auto env = getenv("BROKER_OUTPUT_GENERATOR_FILE"))
    set("broker.output-generator-file", env);
  if (auto env = getenv("BROKER_OUTPUT_GENERATOR_FILE_CAP")) {
    try {
      auto value = static_cast<size_t>(std::stoi(env));
      if (value < 0)
        throw std::runtime_error("expected a positive number");
      set("broker.output-generator-file-cap", static_cast<size_t>(value));
    } catch (...) {
      std::cerr << "*** invalid value for BROKER_OUTPUT_GENERATOR_FILE_CAP: "
                << env << " (expected a positive number)";
    }
  }
}

configuration::configuration(int argc, char** argv) : configuration{} {
  parse(argc, argv);
}

#define ADD_MSG_TYPE(name) cfg.add_message_type<name>(#name)

void configuration::add_message_types(caf::actor_system_config& cfg) {
  ADD_MSG_TYPE(broker::data);
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
  ADD_MSG_TYPE(std::vector<broker::topic>);
  ADD_MSG_TYPE(broker::optional<broker::timestamp>);
  ADD_MSG_TYPE(broker::optional<broker::timespan>);
  ADD_MSG_TYPE(broker::snapshot);
  ADD_MSG_TYPE(broker::internal_command);
  ADD_MSG_TYPE(broker::command_message);
  ADD_MSG_TYPE(broker::data_message);
  ADD_MSG_TYPE(broker::node_message);
  ADD_MSG_TYPE(broker::set_command);
  ADD_MSG_TYPE(broker::store::stream_type::value_type);
  ADD_MSG_TYPE(std::vector<broker::store::stream_type::value_type>);
  ADD_MSG_TYPE(broker::endpoint::stream_type::value_type);
  ADD_MSG_TYPE(std::vector<broker::endpoint::stream_type::value_type>);
}

#undef ADD_MSG_TYPE

} // namespace broker
