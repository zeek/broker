#include <pybind11/functional.h>
#include <pybind11/operators.h>
#include <pybind11/pybind11.h>

#include "broker/broker.hh"

using namespace broker;

namespace py = pybind11;
using namespace pybind11::literals;

PYBIND11_PLUGIN(_broker) {
  py::module m{"_broker", "Broker python bindings"};

  //
  // Version & Constants
  //

  auto version = m.def_submodule("Version", "Version constants");
  version.attr("MAJOR") = py::cast(new version::type{version::major});
  version.attr("MINOR") = py::cast(new version::type{version::minor});
  version.attr("PATCH") = py::cast(new version::type{version::patch});
  version.attr("PROTOCOL") = py::cast(new version::type{version::protocol});
  version.def("compatible", &version::compatible,
              "Checks whether two Broker protocol versions are compatible");

  py::enum_<ec>(m, "EC")
    .value("Unspecified", ec::unspecified)
    .value("VersionIncompatible", ec::version_incompatible)
    .value("MasterExists", ec::master_exists)
    .value("NoSuchMaster", ec::no_such_master)
    .value("TypeClash", ec::type_clash)
    .value("InvalidData", ec::invalid_data)
    .value("BackendFailure", ec::backend_failure);

  py::enum_<peer_status>(m, "PeerStatus")
    .value("Initialized", peer_status::initialized)
    .value("Connecting", peer_status::connecting)
    .value("Connected", peer_status::connected)
    .value("Peered", peer_status::peered)
    .value("Disconnected", peer_status::disconnected)
    .value("Reconnecting", peer_status::reconnecting);

  py::enum_<peer_flags>(m, "PeerFlags")
    .value("Invalid", peer_flags::invalid)
    .value("Local", peer_flags::local)
    .value("Remote", peer_flags::remote)
    .value("Outbound", peer_flags::outbound)
    .value("Inbound", peer_flags::inbound);

  py::enum_<status_info>(m, "StatusInfo")
    .value("UnknownStatus", unknown_status)
    .value("PeerAdded", peer_added)
    .value("PeerRemoved", peer_removed)
    .value("PeerIncompatible", peer_incompatible)
    .value("PeerInvalid", peer_invalid)
    .value("PeerUnavailable", peer_unavailable)
    .value("PeerLost", peer_lost)
    .value("PeerRecovered", peer_recovered)
    .export_values();

  py::enum_<api_flags>(m, "ApiFlags")
    .value("Blocking", blocking)
    .value("Nonblocking", nonblocking)
    .export_values();

  py::enum_<frontend>(m, "Frontend")
    .value("Master", master)
    .value("Clone", clone)
    .export_values();

  py::enum_<backend>(m, "Backend")
    .value("Memory", memory)
    .value("SQLite", sqlite)
    .value("RocksDB", rocksdb)
    .export_values();

  //
  // General
  //

  py::class_<error>(m, "Error")
    .def("code", &error::code)
    .def("context", [](const error& e) { return e.context(); });

  py::class_<endpoint_info>(m, "EndpointInfo")
    .def_readwrite("node", &endpoint_info::node)
    .def_readwrite("id", &endpoint_info::id)
    .def_readwrite("network", &endpoint_info::network);

  py::class_<network_info>(m, "NetworkInfo")
    .def_readwrite("address", &network_info::address)
    .def_readwrite("port", &network_info::port);

  py::class_<peer_info>(m, "PeerInfo")
    .def_readwrite("peer", &peer_info::peer)
    .def_readwrite("flags", &peer_info::flags)
    .def_readwrite("status", &peer_info::status);

  py::class_<status>(m, "Status")
    .def(py::init<status_info>())
    .def_readwrite("info", &status::info)
    .def_readwrite("endpoint", &status::endpoint)
    .def_readwrite("message", &status::message);

  //
  // Data model
  //

  // A thin wrapper around the 'count' type, because Python has no notion of
  // unsigned integers.
  struct count_type {
    count_type(count c) : value{c} {
    }

    count value;
  };

  py::class_<count_type>(m, "Count")
    .def(py::init<py::int_>());

  py::class_<interval>(m, "Interval")
    .def(py::init<>())
    .def(py::init<integer>())
    .def("__init__",
         [](interval& instance, double seconds) {
           auto fs = fractional_seconds{seconds};
           auto i = std::chrono::duration_cast<interval>(fs);
           new (&instance) interval{i};
         })
    .def("count", &interval::count)
    .def("__repr__", [](const interval& i) { return to_string(i); })
    .def(py::self + py::self)
    .def(py::self - py::self)
    .def(py::self * interval::rep{})
    .def(interval::rep{} * py::self)
    .def(py::self / py::self)
    .def(py::self / interval::rep{})
    .def(py::self % py::self)
    .def(py::self % interval::rep{})
    .def(py::self < py::self)
    .def(py::self <= py::self)
    .def(py::self > py::self)
    .def(py::self >= py::self)
    .def(py::self == py::self)
    .def(py::self != py::self);

  py::class_<timestamp>(m, "Timestamp")
    .def(py::init<>())
    .def(py::init<interval>())
    .def("__init__",
         [](timestamp& instance, double seconds) {
           auto fs = fractional_seconds{seconds};
           auto i = std::chrono::duration_cast<interval>(fs);
           new (&instance) timestamp{i};
         })
    .def("time_since_epoch", &timestamp::time_since_epoch)
    .def("__repr__", [](const timestamp& ts) { return to_string(ts); })
    .def(py::self < py::self)
    .def(py::self <= py::self)
    .def(py::self > py::self)
    .def(py::self >= py::self)
    .def(py::self == py::self)
    .def(py::self != py::self);

  m.def("now", &now, "Get the current wallclock time");

  py::class_<address> address_type{m, "Address"};
  address_type
    .def(py::init<>())
    .def("__init__",
         [](address& instance, const py::bytes& bytes, int family) {
           BROKER_ASSERT(family == 4 || family == 6);
           auto str = static_cast<std::string>(bytes);
           auto ptr = reinterpret_cast<const uint32_t*>(str.data());
           auto f = family == 4 ? address::family::ipv4 : address::family::ipv6;
           new (&instance) address{ptr, f, address::byte_order::network};
         })
    .def("mask", &address::mask, "top_bits_to_keep"_a)
    .def("v4", &address::is_v4)
    .def("v6", &address::is_v6)
    .def("bytes", &address::bytes)
    .def("__repr__", [](const address& a) { return to_string(a); })
    .def(py::self < py::self)
    .def(py::self <= py::self)
    .def(py::self > py::self)
    .def(py::self >= py::self)
    .def(py::self == py::self)
    .def(py::self != py::self);

  py::enum_<address::family>(address_type, "Family")
    .value("IPv4", address::family::ipv4)
    .value("IPv6", address::family::ipv6);

  py::enum_<address::byte_order>(address_type, "ByteOrder")
    .value("Host", address::byte_order::host)
    .value("Network", address::byte_order::network);

  py::class_<subnet>(m, "Subnet")
    .def(py::init<>())
    .def("__init__",
         [](subnet& instance, address addr, uint8_t length) {
           new (&instance) subnet{std::move(addr), length};
         })
    .def("contains", &subnet::contains, "addr"_a)
    .def("network", &subnet::network)
    .def("length", &subnet::length)
    .def("__repr__", [](const subnet& sn) { return to_string(sn); })
    .def(py::self < py::self)
    .def(py::self <= py::self)
    .def(py::self > py::self)
    .def(py::self >= py::self)
    .def(py::self == py::self)
    .def(py::self != py::self);

  py::class_<port> port_type{m, "Port"};
  port_type
    .def(py::init<>())
    .def(py::init<port::number_type, port::protocol>())
    .def("number", &port::number)
    .def("type", &port::type)
    .def("__repr__", [](const port& p) { return to_string(p); })
    .def(py::self < py::self)
    .def(py::self <= py::self)
    .def(py::self > py::self)
    .def(py::self >= py::self)
    .def(py::self == py::self)
    .def(py::self != py::self);

  py::enum_<port::protocol>(port_type, "Protocol")
    .value("TCP", port::protocol::tcp)
    .value("UDP", port::protocol::udp)
    .value("ICMP", port::protocol::icmp)
    .value("Unknown", port::protocol::unknown)
    .export_values();

  py::class_<data>(m, "Data")
    .def(py::init<>())
    .def(py::init<boolean>())
    .def(py::init<integer>())
    .def("__init__",
         [](data& instance, count_type c) { new (&instance) data{c.value}; })
    .def(py::init<real>())
    .def(py::init<interval>())
    .def(py::init<timestamp>())
    .def(py::init<std::string>())
    .def(py::init<address>())
    .def(py::init<subnet>())
    .def(py::init<port>())
    .def(py::init<vector>())
    .def(py::init<set>())
    .def(py::init<table>())
    .def("__str__", [](const data& d) { return to_string(d); })
    .def(py::self < py::self)
    .def(py::self <= py::self)
    .def(py::self > py::self)
    .def(py::self >= py::self)
    .def(py::self == py::self)
    .def(py::self != py::self);

  // py::bind_vector<data> (from pybind11/stl_bind.h) causes an infinite
  // recursion in __repr__. See #371 for details.
  py::class_<vector>(m, "Vector")
    .def(py::init<>())
    .def("__init__",
         [](vector& instance, const py::list& list) {
           new (&instance) vector(list.size());
           try {
             for (auto i = 0u; i < instance.size(); ++i)
               instance[i] = list[i].cast<data>();
           } catch (...) {
             instance.~vector();
             throw;
           }
         })
    .def(py::self < py::self)
    .def(py::self <= py::self)
    .def(py::self > py::self)
    .def(py::self >= py::self)
    .def(py::self == py::self)
    .def(py::self != py::self);

  // Don't include pybind11/stl.h, as it will inject the wrong py::type_caster
  // template specializations.
  py::class_<set>(m, "Set")
    .def(py::init<>())
    .def("__init__",
         [](set& instance, const py::list& list) {
           new (&instance) set{};
           try {
             for (auto i = 0u; i < list.size(); ++i)
               instance.insert(list[i].cast<data>());
           } catch (...) {
             instance.~set();
             throw;
           }
         })
    .def(py::self < py::self)
    .def(py::self <= py::self)
    .def(py::self > py::self)
    .def(py::self >= py::self)
    .def(py::self == py::self)
    .def(py::self != py::self);

  py::class_<table>(m, "Table")
    .def(py::init<>())
    .def("__init__",
         [](table& instance, const py::dict& dict) {
           new (&instance) table{};
           try {
             for (auto pair : dict)
               instance.emplace(pair.first.cast<data>(),
                                pair.second.cast<data>());
           } catch (...) {
             instance.~table();
             throw;
           }
         })
    .def(py::self < py::self)
    .def(py::self <= py::self)
    .def(py::self > py::self)
    .def(py::self >= py::self)
    .def(py::self == py::self)
    .def(py::self != py::self);

  //
  // Communication & Store
  //

  py::class_<topic>(m, "Topic")
    .def(py::init<std::string>())
    .def("string", &topic::string,
         "Get the underlying string representation of the topic",
         py::return_value_policy::reference_internal)
    .def("__repr__", [](const topic& t) { return t.string(); });

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
         py::keep_alive<1, 0>())
    .def("spawn_nonblocking", &context::spawn<nonblocking>,
         py::keep_alive<1, 0>());

  py::class_<backend_options>(m, "BackendOptions");

  py::class_<endpoint>(m, "Endpoint")
    .def("listen", &endpoint::listen)
    .def("peer", (void (endpoint::*)(const endpoint&)) &endpoint::peer)
    .def("peer", (void (endpoint::*)(const std::string&, uint16_t))
                   &endpoint::peer)
    .def("unpeer", (void (endpoint::*)(const endpoint&)) &endpoint::unpeer)
    .def("unpeer", (void (endpoint::*)(const std::string&, uint16_t))
                     &endpoint::unpeer)
    .def("peers", &endpoint::peers)
    .def("publish", (void (endpoint::*)(topic, data)) &endpoint::publish,
         "topic"_a, "data"_a)
    .def("attach_master",
         [](endpoint& ep, const std::string& name, backend b,
            const backend_options& opts) {
           switch (b) {
             case memory:
               return ep.attach<master, memory>(name, opts);
             case sqlite:
               return ep.attach<master, sqlite>(name, opts);
             case rocksdb:
               return ep.attach<master, rocksdb>(name, opts);
           }
         },
         py::keep_alive<1, 0>())
    .def("attach_clone",
         [](endpoint& ep, const std::string& name) {
           return ep.attach<clone>(name);
         },
         py::keep_alive<1, 0>());

  py::class_<mailbox>(m, "Mailbox")
    .def("descriptor", &mailbox::descriptor)
    .def("empty", &mailbox::empty)
    .def("count", &mailbox::count);

  py::class_<blocking_endpoint>(m, "BlockingEndpoint", py::base<endpoint>{})
    .def("subscribe", &blocking_endpoint::subscribe)
    .def("unsubscribe", &blocking_endpoint::subscribe)
    .def("receive", (message (endpoint::*)())&blocking_endpoint::receive)
    .def("receive",
         [](blocking_endpoint& ep,
            std::function<void(const topic&, const data& d)> f) {
           ep.receive(f);
         })
    .def("receive",
         [](blocking_endpoint& ep, std::function<void(const status&)> f) {
           ep.receive(f);
         })
    .def("receive",
         [](blocking_endpoint& ep,
            std::function<void(const topic&, const data& d)> on_msg,
            std::function<void(const status&)> on_status
            ) {
           ep.receive(on_msg, on_status);
         })
    .def("mailbox", &blocking_endpoint::mailbox, py::keep_alive<1, 0>());

  py::class_<nonblocking_endpoint>(m, "NonblockingEndpoint",
                                   py::base<endpoint>{})
    .def("subscribe",
         [](nonblocking_endpoint& ep, topic t,
            std::function<void(const topic&, const data& d)> f) {
           ep.subscribe(std::move(t), f);
         })
    .def("subscribe",
         [](nonblocking_endpoint& ep, std::function<void(const status& s)> f) {
           ep.subscribe(f);
         })
    .def("unsubscribe", &blocking_endpoint::subscribe);

  // TODO: complete definition
  py::class_<store>(m, "Store")
    .def("name", &store::name);

  return m.ptr();
}
