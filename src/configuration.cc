#include <caf/io/middleman.hpp>

#include "broker/address.hh"
#include "broker/configuration.hh"
#include "broker/data.hh"
#include "broker/port.hh"
#include "broker/snapshot.hh"
#include "broker/status.hh"
#include "broker/subnet.hh"
#include "broker/time.hh"
#include "broker/topic.hh"
#include "broker/version.hh"

namespace broker {

configuration::configuration() {
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
  add_message_type<snapshot>("broker::snapshot");
  load<caf::io::middleman>();
  logger_filename = "broker_[TIMESTAMP]_[PID].log";
  logger_verbosity = caf::atom("DEBUG");
  middleman_app_identifier = "broker.v" + std::to_string(version::protocol);
}

configuration::configuration(int argc, char** argv) : configuration{} {
  parse(argc, argv);
}

} // namespace broker
