
#include <pybind11/functional.h>
#include <pybind11/operators.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl_bind.h>

#include "set_bind.h"

#include "broker/broker.hh"

namespace py = pybind11;
using namespace pybind11::literals;

// PYBIND11_MAKE_OPAQUE(broker::set);
PYBIND11_MAKE_OPAQUE(broker::table);
PYBIND11_MAKE_OPAQUE(broker::vector);

PYBIND11_PLUGIN(_broker) {
  py::module m{"_broker", "Broker python bindings"};

  auto version = m.def_submodule("Version", "Version constants");
  version.attr("MAJOR")
    = py::cast(new broker::version::type{broker::version::major});
  version.attr("MINOR")
    = py::cast(new broker::version::type{broker::version::minor});
  version.attr("PATCH")
    = py::cast(new broker::version::type{broker::version::patch});
  version.attr("PROTOCOL")
    = py::cast(new broker::version::type{broker::version::protocol});
  version.def("compatible", &broker::version::compatible,
              "Checks whether two Broker protocol versions are compatible");

  m.def("now", &broker::now, "Get the current wallclock time");

  py::enum_<broker::ec>(m, "EC")
    .value("Unspecified", broker::ec::unspecified)
    .value("PeerIncompatible", broker::ec::peer_incompatible)
    .value("PeerInvalid", broker::ec::peer_invalid)
    .value("PeerUnavailable", broker::ec::peer_unavailable)
    .value("PeerTimeout", broker::ec::peer_timeout)
    .value("MasterExists", broker::ec::master_exists)
    .value("NoSuchMaster", broker::ec::no_such_master)
    .value("NoSuchKey", broker::ec::no_such_key)
    .value("RequestTimeOut", broker::ec::request_timeout)
    .value("TypeClash", broker::ec::type_clash)
    .value("InvalidData", broker::ec::invalid_data)
    .value("BackendFailure", broker::ec::backend_failure);

  py::enum_<broker::sc>(m, "SC")
    .value("Unspecified", broker::sc::unspecified)
    .value("PeerAdded", broker::sc::peer_added)
    .value("PeerRemoved", broker::sc::peer_removed)
    .value("PeerLost", broker::sc::peer_lost);

  py::enum_<broker::peer_status>(m, "PeerStatus")
    .value("Initialized", broker::peer_status::initialized)
    .value("Connecting", broker::peer_status::connecting)
    .value("Connected", broker::peer_status::connected)
    .value("Peered", broker::peer_status::peered)
    .value("Disconnected", broker::peer_status::disconnected)
    .value("Reconnecting", broker::peer_status::reconnecting);

  py::enum_<broker::peer_flags>(m, "PeerFlags")
    .value("Invalid", broker::peer_flags::invalid)
    .value("Local", broker::peer_flags::local)
    .value("Remote", broker::peer_flags::remote)
    .value("Outbound", broker::peer_flags::outbound)
    .value("Inbound", broker::peer_flags::inbound);

  py::enum_<broker::api_flags>(m, "APIFlags")
    .value("Blocking", broker::blocking)
    .value("NonBlocking", broker::nonblocking)
    .export_values();

  py::enum_<broker::frontend>(m, "Frontend")
    .value("Master", broker::master)
    .value("Clone", broker::clone)
    .export_values();

  py::enum_<broker::backend>(m, "Backend")
    .value("Memory", broker::memory)
    .value("SQLite", broker::sqlite)
    .value("RocksDB", broker::rocksdb)
    .export_values();

  py::class_<broker::endpoint_info>(m, "EndpointInfo")
    .def_readwrite("node", &broker::endpoint_info::node)
    .def_readwrite("network", &broker::endpoint_info::network);

  py::class_<broker::network_info>(m, "NetworkInfo")
    .def_readwrite("address", &broker::network_info::address)
    .def_readwrite("port", &broker::network_info::port);

  py::class_<broker::peer_info>(m, "PeerInfo")
    .def_readwrite("peer", &broker::peer_info::peer)
    .def_readwrite("flags", &broker::peer_info::flags)
    .def_readwrite("status", &broker::peer_info::status);

  py::class_<broker::topic>(m, "Topic")
    .def(py::init<std::string>())
    .def(py::self /= py::self,
         "Appends a topic component with a separator")
    .def(py::self / py::self,
         "Appends topic components with a separator")
    .def("string", &broker::topic::string,
         "Get the underlying string representation of the topic",
         py::return_value_policy::reference_internal)
    .def("__repr__", [](const broker::topic& t) { return t.string(); });

  py::bind_vector<std::vector<broker::topic>>(m, "VectorTopic");

  py::class_<broker::infinite_t>(m, "Infinite")
    .def(py::init<>());

  //
  // Data model
  //

  py::class_<broker::address> address_type{m, "Address"};
  address_type.def(py::init<>())
    .def("__init__",
         [](broker::address& instance, const py::bytes& bytes, int family) {
           BROKER_ASSERT(family == 4 || family == 6);
           auto str = static_cast<std::string>(bytes);
           auto ptr = reinterpret_cast<const uint32_t*>(str.data());
           auto f = family == 4 ? broker::address::family::ipv4 :
                                  broker::address::family::ipv6;
           new (&instance)
             broker::address{ptr, f, broker::address::byte_order::network};
         })
    .def("mask", &broker::address::mask, "top_bits_to_keep"_a)
    .def("is_v4", &broker::address::is_v4)
    .def("is_v6", &broker::address::is_v6)
    .def("bytes", [](const broker::address& a) {
        return py::bytes(std::string(std::begin(a.bytes()), std::end(a.bytes())));
        })
    .def("__repr__",
         [](const broker::address& a) { return broker::to_string(a); })
    .def(py::self < py::self)
    .def(py::self <= py::self)
    .def(py::self > py::self)
    .def(py::self >= py::self)
    .def(py::self == py::self)
    .def(py::self != py::self);

  py::enum_<broker::address::family>(address_type, "Family")
    .value("IPv4", broker::address::family::ipv4)
    .value("IPv6", broker::address::family::ipv6);

  py::enum_<broker::address::byte_order>(address_type, "ByteOrder")
    .value("Host", broker::address::byte_order::host)
    .value("Network", broker::address::byte_order::network);

  // A thin wrapper around the 'count' type, because Python has no notion of
  // unsigned integers.
  struct count_type {
    count_type(broker::count c) : value{c} {}
    bool operator==(const count_type& other) const { return value == other.value; }
    bool operator!=(const count_type& other) const { return value != other.value; }
    bool operator<(const count_type& other) const { return value < other.value; }
    bool operator<=(const count_type& other) const { return value <= other.value; }
    bool operator>(const count_type& other) const { return value > other.value; }
    bool operator>=(const count_type& other) const { return value >= other.value; }
    broker::count value;
  };

  py::class_<count_type>(m, "Count")
    .def(py::init<py::int_>())
    .def_readwrite("value", &count_type::value)
    .def(py::self < py::self)
    .def(py::self <= py::self)
    .def(py::self > py::self)
    .def(py::self >= py::self)
    .def(py::self == py::self)
    .def(py::self != py::self);

  py::class_<broker::enum_value>{m, "Enum"}
    .def(py::init<std::string>())
    .def_readwrite("name", &broker::enum_value::name)
    .def("__repr__", [](const broker::enum_value& e) { return broker::to_string(e); })
    .def(py::self < py::self)
    .def(py::self <= py::self)
    .def(py::self > py::self)
    .def(py::self >= py::self)
    .def(py::self == py::self)
    .def(py::self != py::self);

  py::class_<broker::port> port_type{m, "Port"};
  port_type
    .def(py::init<>())
    .def(py::init<broker::port::number_type, broker::port::protocol>())
    .def("number", &broker::port::number)
    .def("get_type", &broker::port::type)
    .def("__repr__", [](const broker::port& p) { return broker::to_string(p); })
    .def(py::self < py::self)
    .def(py::self <= py::self)
    .def(py::self > py::self)
    .def(py::self >= py::self)
    .def(py::self == py::self)
    .def(py::self != py::self);

  py::enum_<broker::port::protocol>(port_type, "Protocol")
    .value("ICMP", broker::port::protocol::icmp)
    .value("TCP", broker::port::protocol::tcp)
    .value("UDP", broker::port::protocol::udp)
    .value("Unknown", broker::port::protocol::unknown)
    .export_values();

  py::bind_set<broker::set>(m, "Set");

  py::bind_map<broker::table>(m, "Table");

  py::class_<broker::subnet>(m, "Subnet")
    .def(py::init<>())
    .def("__init__",
         [](broker::subnet& instance, broker::address addr, uint8_t length) {
           new (&instance) broker::subnet{std::move(addr), length};
         })
    .def("contains", &broker::subnet::contains, "addr"_a)
    .def("network", &broker::subnet::network)
    .def("length", &broker::subnet::length)
    .def("__repr__", [](const broker::subnet& sn) { return to_string(sn); })
    .def(py::self < py::self)
    .def(py::self <= py::self)
    .def(py::self > py::self)
    .def(py::self >= py::self)
    .def(py::self == py::self)
    .def(py::self != py::self);

  py::class_<broker::timespan>(m, "Timespan")
    .def(py::init<>())
    .def(py::init<broker::integer>())
    .def("__init__",
         [](broker::timespan& instance, double secs) {
           new (&instance) broker::timespan{broker::to_timespan(secs)};
         })
    .def("count", &broker::timespan::count)
    .def("__repr__", [](const broker::timespan& s) { return broker::to_string(s); })
    .def(py::self + py::self)
    .def(py::self - py::self)
    .def(py::self * broker::timespan::rep{})
    .def(broker::timespan::rep{} * py::self)
    .def(py::self / py::self)
    .def(py::self / broker::timespan::rep{})
    .def(py::self % py::self)
    .def(py::self % broker::timespan::rep{})
    .def(py::self < py::self)
    .def(py::self <= py::self)
    .def(py::self > py::self)
    .def(py::self >= py::self)
    .def(py::self == py::self)
    .def(py::self != py::self);

  py::class_<broker::timestamp>(m, "Timestamp")
    .def(py::init<>())
    .def(py::init<broker::timespan>())
    .def("__init__",
         [](broker::timestamp& instance, double secs) {
           new (&instance) broker::timespan{broker::to_timespan(secs)};
         })
    .def("time_since_epoch", &broker::timestamp::time_since_epoch)
    .def("__repr__", [](const broker::timestamp& ts) { return broker::to_string(ts); })
    .def(py::self < py::self)
    .def(py::self <= py::self)
    .def(py::self > py::self)
    .def(py::self >= py::self)
    .def(py::self == py::self)
    .def(py::self != py::self);

  py::bind_vector<broker::vector>(m, "Vector");

  py::class_<broker::data> data_type{m, "Data"};
  data_type
    .def(py::init<>())
    .def(py::init<broker::data>())
    .def(py::init<broker::address>())
    .def(py::init<broker::boolean>())
    .def("__init__",
         [](broker::data& instance, count_type c) { new (&instance) broker::data{c.value}; })
    .def("__init__",
         [](broker::data& instance, broker::enum_value e) { new (&instance) broker::data{e}; })
    .def(py::init<broker::integer>())
    .def(py::init<broker::port>())
    .def(py::init<broker::real>())
    .def(py::init<broker::set>())
    .def(py::init<std::string>())
    .def(py::init<broker::subnet>())
    .def(py::init<broker::table>())
    .def(py::init<broker::timespan>())
    .def(py::init<broker::timestamp>())
    .def(py::init<broker::vector>())
    .def("as_address", [](const broker::data& d) { return broker::get<broker::address>(d); })
    .def("as_boolean", [](const broker::data& d) { return broker::get<broker::boolean>(d); })
    .def("as_count", [](const broker::data& d) { return broker::get<broker::count>(d); })
    .def("as_enum_value", [](const broker::data& d) { return broker::get<broker::enum_value>(d); })
    .def("as_integer", [](const broker::data& d) { return broker::get<broker::integer>(d); })
    .def("as_none", [](const broker::data& d) { return broker::get<broker::none>(d); })
    .def("as_port", [](const broker::data& d) { return broker::get<broker::port>(d); })
    .def("as_real", [](const broker::data& d) { return broker::get<broker::real>(d); })
    .def("as_set", [](const broker::data& d) { return broker::get<broker::set>(d); })
    .def("as_string", [](const broker::data& d) { return broker::get<std::string>(d); })
    .def("as_subnet", [](const broker::data& d) { return broker::get<broker::subnet>(d); })
    .def("as_table", [](const broker::data& d) { return broker::get<broker::table>(d); })
    .def("as_timespan", [](const broker::data& d) { return broker::get<broker::timespan>(d); })
    .def("as_timestamp", [](const broker::data& d) { return broker::get<broker::timestamp>(d); })
    .def("as_vector", [](const broker::data& d) { return broker::get<broker::vector>(d); })
    .def("get_type", &broker::data::get_type)
    .def("__str__", [](const broker::data& d) { return broker::to_string(d); })
    .def(py::self < py::self)
    .def(py::self <= py::self)
    .def(py::self > py::self)
    .def(py::self >= py::self)
    .def(py::self == py::self)
    .def(py::self != py::self);

  py::enum_<broker::data::type>(data_type, "Type")
    .value("Nil", broker::data::type::none)
    .value("Address", broker::data::type::address)
    .value("Boolean", broker::data::type::boolean)
    .value("Count", broker::data::type::count)
    .value("EnumValue", broker::data::type::enum_value)
    .value("Integer", broker::data::type::integer)
    .value("None", broker::data::type::none)
    .value("Port", broker::data::type::port)
    .value("Real", broker::data::type::real)
    .value("Set", broker::data::type::set)
    .value("String", broker::data::type::string)
    .value("Subnet", broker::data::type::subnet)
    .value("Table", broker::data::type::table)
    .value("Timespan", broker::data::type::timespan)
    .value("Timestamp", broker::data::type::timestamp)
    .value("Vector", broker::data::type::vector);

  using subscriber_base = broker::subscriber_base<std::pair<broker::topic, broker::data>>;

  py::bind_vector<std::vector<subscriber_base::value_type>>(m, "VectorSubscriberValueType");

  py::class_<subscriber_base>(m, "SubscriberBase")
    .def("get", (subscriber_base::value_type (subscriber_base::*)()) &subscriber_base::get)
    .def("get",
         [](subscriber_base& ep, double secs) -> broker::optional<subscriber_base::value_type> {
	   return ep.get(broker::to_duration(secs)); })
    .def("get",
         [](subscriber_base& ep, size_t num) -> std::vector<subscriber_base::value_type> {
	   return ep.get(num); })
    .def("get",
         [](subscriber_base& ep, size_t num, double secs) -> std::vector<subscriber_base::value_type> {
	   return ep.get(num, broker::to_duration(secs)); })
    .def("poll", &subscriber_base::poll)
    .def("available", &subscriber_base::available)
    .def("fd", &subscriber_base::fd);

  py::class_<broker::subscriber, subscriber_base>(m, "Subscriber")
    .def("add_topic", &broker::subscriber::add_topic)
    .def("remove_topic", &broker::subscriber::remove_topic);

  using event_subscriber_base = broker::subscriber_base<broker::detail::variant<broker::none, broker::error, broker::status>>;

  py::class_<event_subscriber_base>(m, "EventSubscriberBase")
    .def("get", (event_subscriber_base::value_type (event_subscriber_base::*)()) &event_subscriber_base::get)
    .def("get", (broker::optional<event_subscriber_base::value_type> (event_subscriber_base::*)(broker::duration)) &broker::event_subscriber::get)
    .def("get", (std::vector<event_subscriber_base::value_type> (event_subscriber_base::*)(size_t num, broker::duration)) &broker::event_subscriber::get, py::arg("num"), py::arg("timeout") = broker::infinite)
    .def("poll", &event_subscriber_base::poll)
    .def("available", &event_subscriber_base::available)
    .def("fd", &event_subscriber_base::fd);

  py::class_<broker::event_subscriber, event_subscriber_base>(m, "EventSubscriber");

  py::class_<broker::endpoint>(m, "Endpoint")
    .def(py::init<>())
    // .def(py::init<broker::configuration>())
    .def("listen", &broker::endpoint::listen, py::arg("address"), py::arg("port") = 0)
    .def("peer",
         [](broker::endpoint& ep, std::string& addr, uint16_t port, double secs) {
	 ep.peer(addr, port, std::chrono::seconds((int)secs));
         })
    .def("unpeer", &broker::endpoint::peer)
    .def("peers", &broker::endpoint::peers)
    .def("peer_subscriptions", &broker::endpoint::peer_subscriptions)
    .def("publish", (void (broker::endpoint::*)(broker::topic t, broker::data d)) &broker::endpoint::publish)
    .def("publish", (void (broker::endpoint::*)(const broker::endpoint_info& dst, broker::topic t, broker::data d)) &broker::endpoint::publish)
    // .def("publish", (void (endpoint::*)(topic t, std::initializer_list<data> xs)) &broker::endpoint::publish
    // .def("publish", (void (endpoint::*)(std::vector<value_type> xs)) &broker::endpoint::publish
    // .def("make_publisher", &broker::endpoint::make_publisher);
    // .def("publish_all", ...)
    // .def("publish_all_nosync", ...)
    .def("make_event_subscriber", &broker::endpoint::make_event_subscriber, py::arg("receive_statuses") = false)
    .def("make_subscriber", &broker::endpoint::make_subscriber, py::arg("topics"), py::arg("max_qsize") = 20)
    // .def("subscribe", ...)
    // .def("subscribe_nosync", ...)
    .def("shutdown", &broker::endpoint::shutdown)
    ;

#if 0
    // TODO: old, needs updating.
    .def("attach_master",
         [](endpoint& ep, const std::string& name, backend b,
            const backend_options& opts) -> expected<store> {
           switch (b) {
             default:
               return make_error(ec::backend_failure, "invalid backend type");
             case memory:
               return ep.attach<master, memory>(name, opts);
             case sqlite:
               return ep.attach<master, sqlite>(name, opts);
             case rocksdb:
               return ep.attach<master, rocksdb>(name, opts);
           }
         },
         py::keep_alive<0, 1>())
    .def("attach_clone",
         [](endpoint& ep, const std::string& name) {
           return ep.attach<broker::clone>(name);
         },
         py::keep_alive<0, 1>());
#endif

/////// TODO: Updated to new Broker API until here.

#if 0
  py::class_<status>(m, "Status")
    .def(py::init<>())
    .def("context",
         [](status& instance) {
           // TODO: create the right context object based on status code.
           return "";
         })
    .def("message",
         [](status& instance) {
           auto msg = instance.message();
           return msg ? *msg : std::string{};
         });

  //
  // Communication & Store
  //

  py::class_<message>(m, "Message")
    .def("topic", &message::topic)
    .def("data", &message::data);

  // TODO: add ctor that takes command line arguments.
  py::class_<configuration>(m, "Configuration")
    .def(py::init<>());

  py::class_<context>(m, "Context")
    .def("__init__",
         [](context& instance) {
           new (&instance) context{};
         })
    .def("__init__",
         [](context& instance, configuration& cfg) {
           new (&instance) context{std::move(cfg)};
         })
    .def("spawn_blocking", &context::spawn<blocking>,
         py::keep_alive<0, 1>())
    .def("spawn_nonblocking", &context::spawn<nonblocking>,
         py::keep_alive<0, 1>());

  py::class_<backend_options>(m, "BackendOptions");

  py::class_<mailbox>(m, "Mailbox")
    .def("descriptor", &mailbox::descriptor)
    .def("empty", &mailbox::empty)
    .def("count", &mailbox::count);

  py::class_<blocking_endpoint, endpoint>(m, "BlockingEndpoint")
    .def("subscribe", [](blocking_endpoint& ep, const std::string& t) {
           ep.subscribe(t);
         })
    .def("unsubscribe", [](blocking_endpoint& ep, const std::string& t) {
           ep.unsubscribe(t);
         })
    .def("receive", &blocking_endpoint::receive)
    .def("mailbox", &blocking_endpoint::mailbox, py::keep_alive<0, 1>());

  py::class_<nonblocking_endpoint, endpoint>(m, "NonblockingEndpoint")
    .def("subscribe_msg",
         [](nonblocking_endpoint& ep, const std::string& t,
            std::function<void(const topic&, const data& d)> f) {
           ep.subscribe(t, f);
         })
    .def("subscribe_status",
         [](nonblocking_endpoint& ep, std::function<void(const status& s)> f) {
           ep.subscribe(f);
         })
    .def("unsubscribe", [](nonblocking_endpoint& ep, const std::string& t) {
           ep.unsubscribe(t);
         });

  // TODO: complete definition
  py::class_<store>(m, "Store")
    .def("name", &store::name);
#endif

  return m.ptr();
}

