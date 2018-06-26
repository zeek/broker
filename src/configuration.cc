#include <cstdlib>
#include <cstring>
#include <utility>
#include <vector>

#include <caf/io/middleman.hpp>
#include <caf/openssl/manager.hpp>
#include <caf/atom.hpp>

#include "broker/version.hh"
#include "broker/data.hh"
#include "broker/endpoint.hh"
#include "broker/snapshot.hh"
#include "broker/store.hh"
#include "broker/time.hh"
#include "broker/address.hh"
#include "broker/internal_command.hh"
#include "broker/port.hh"
#include "broker/status.hh"
#include "broker/subnet.hh"
#include "broker/topic.hh"

#include "broker/configuration.hh"

namespace broker {

configuration::configuration(broker_options opts) : options_(std::move(opts)) {
  add_message_type<data>("broker::data");
  add_message_type<address>("broker::address");
  add_message_type<subnet>("broker::subnet");
  add_message_type<port>("broker::port");
  add_message_type<timespan>("broker::timespan");
  add_message_type<timestamp>("broker::timestamp");
  add_message_type<enum_value>("broker::enum_value");
  add_message_type<vector>("broker::vector");
  add_message_type<broker::set>("broker::set");
  add_message_type<status>("broker::status");
  add_message_type<table>("broker::table");
  add_message_type<topic>("broker::topic");
  add_message_type<std::vector<topic>>("std::vector<broker::topic>");
  add_message_type<optional<timestamp>>("broker::optional<broker::timestamp>");
  add_message_type<optional<timespan>>("broker::optional<broker::timespan>");
  add_message_type<snapshot>("broker::snapshot");
  add_message_type<internal_command>("broker::internal_command");
  add_message_type<set_command>("broker::set_command");
  add_message_type<store::stream_type::value_type>(
    "broker::store::stream_type::value_type");
  add_message_type<std::vector<store::stream_type::value_type>>(
    "std::vector<broker::store::stream_type::value_type>");
  add_message_type<endpoint::stream_type::value_type>(
    "broker::endpoint::stream_type::value_type");
  add_message_type<std::vector<endpoint::stream_type::value_type>>(
    "std::vector<broker::endpoint::stream_type::value_type>");
  load<caf::io::middleman>();
  if (! options_.disable_ssl)
    load<caf::openssl::manager>();

  set("logger.file-name", "broker_[PID]_[TIMESTAMP].log");
  /*
  set("logger.verbosity", caf::atom("INFO"));
  set("logger.component-filter", "broker");
  */

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

  if (auto env = getenv("BROKER_DEBUG_COMPONENT_FILTER")) {
    set("logger.component-filter", env);
  }

  set("middleman.app-identifier",
      "broker.v" + std::to_string(version::protocol));
}

configuration::configuration(int argc, char** argv) : configuration{} {
  parse(argc, argv);
}

} // namespace broker
