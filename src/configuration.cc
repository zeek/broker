#include <caf/io/middleman.hpp>

#include "broker/address.hh"
#include "broker/configuration.hh"
#include "broker/data.hh"
#include "broker/port.hh"
#include "broker/subnet.hh"
#include "broker/time.hh"
#include "broker/topic.hh"

namespace broker {

configuration::configuration() {
  add_message_type<data>("broker::data");
  add_message_type<address>("broker::address");
  add_message_type<subnet>("broker::subnet");
  add_message_type<port>("broker::port");
  add_message_type<time::duration>("broker::time::duration");
  add_message_type<time::point>("broker::time::point");
  add_message_type<enum_value>("broker::enum_value");
  add_message_type<vector>("broker::vector");
  add_message_type<broker::set>("broker::set");
  add_message_type<table>("broker::table");
  add_message_type<record>("broker::record");
  add_message_type<topic>("broker::topic");
  add_message_type<std::vector<topic>>("std::vector<broker::topic>");
//    .add_message_type<store::sequence_num>("broker::store::sequence_num")
//    .add_message_type<store::expiration_time>("broker::store::expiration_time")
//    .add_message_type<store::query>("broker::store::query")
//    .add_message_type<store::response>("broker::store::response")
//    .add_message_type<store::result>("broker::store::result")
//    .add_message_type<store::snapshot>("broker::store::snapshot")
//    .add_message_type<store::value>("broker::store::value")
//    .add_message_type<store::value>("broker::store::value")
  load<caf::io::middleman>();
}

configuration::configuration(int argc, char** argv) : configuration{} {
  parse(argc, argv);
}

} // namespace broker
