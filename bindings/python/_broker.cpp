#include <pybind11/functional.h>
#include <pybind11/operators.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

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

  py::class_<message>(m, "Message");

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

  py::class_<interval>(m, "Interval")
    .def(py::init<>())
    .def(py::init<integer>())
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
    .def("time_since_epoch", &timestamp::time_since_epoch)
    .def("__repr__", [](const timestamp& ts) { return to_string(ts); })
    .def(py::self < py::self)
    .def(py::self <= py::self)
    .def(py::self > py::self)
    .def(py::self >= py::self)
    .def(py::self == py::self)
    .def(py::self != py::self);

  py::class_<enum_value>(m, "EnumValue")
    .def(py::init<std::string>())
    .def_readwrite("name", &enum_value::name);

  py::class_<address>(m, "Address")
    .def(py::init<>())
    // TODO: Add bytes-based ctor overload.
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

  py::class_<port> port_type(m, "Port");
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
    .value("Unknown", port::protocol::unknown);

  // TODO:
  // - How to visit? Decorators?
  // - How to type-inspect and call .get<T>?
  py::class_<data>(m, "Data")
    // TODO: Add missing ctor overloads.
    .def("__init__",
         [](data& instance, py::none) { new (&instance) data{nil}; })
    .def(py::init<none>())
    .def(py::init<boolean>())
    .def(py::init<integer>())
    .def(py::init<count>())
    .def(py::init<real>())
    .def(py::init<interval>())
    .def(py::init<timestamp>())
    .def(py::init<enum_value>())
    .def(py::init<std::string>())
    .def(py::init<address>())
    .def(py::init<subnet>())
    .def(py::init<port>())
    .def(py::init<vector>())
    .def(py::init<set>())
    .def(py::init<table>())
    // TODO
    //.def("get", [](const data& d, py::object obj) {
    //       return dispatch_based_on_type(obj);
    //     })
    .def("__str__", [](const data& d) { return to_string(d); })
    .def(py::self < py::self)
    .def(py::self <= py::self)
    .def(py::self > py::self)
    .def(py::self >= py::self)
    .def(py::self == py::self)
    .def(py::self != py::self);

  py::class_<vector>(m, "Vector")
    .def(py::init<>())
    .def("__getitem__", [](const vector &v, size_t i) {
       if (i >= v.size())
         throw py::index_error();
       return v[i];
     })
    .def("__getitem__",
         [](const vector& v, py::slice slice) -> vector* {
           size_t start, stop, step, slicelength;
           if (!slice.compute(v.size(), &start, &stop, &step, &slicelength))
             throw py::error_already_set();
           auto seq = new vector(slicelength);
           for (auto i = 0u; i < slicelength; ++i) {
             (*seq)[i] = v[start];
             start += step;
           }
           return seq;
         })
    .def("__setitem__", [](vector &v, size_t i, const data& d) {
       if (i >= v.size())
         throw py::index_error();
       v[i] = d;
     })
    .def("__setitem__",
         [](vector& v, py::slice slice, const vector& value) {
           size_t start, stop, step, slicelength;
           if (!slice.compute(v.size(), &start, &stop, &step, &slicelength))
             throw py::error_already_set();
           if (slicelength != value.size())
               throw std::runtime_error("Left and right hand size of slice "
                                        "assignment have different sizes!");
           for (auto i = 0u; i < slicelength; ++i) {
               v[start] = value[i];
               start += step;
           }
         })
    .def("__len__", &vector::size)
    .def("__iter__", [](const vector &v) {
           return py::make_iterator(v.begin(), v.end());
         },
         py::keep_alive<0, 1>())
    .def("__contains__",
         [](const vector& v, const data& d) {
           return std::find(v.begin(), v.end(), d) != v.end();
         })
    .def("__reversed__",
         [](vector v) -> vector {
           std::reverse(v.begin(), v.end());
           return v;
         })
    .def(py::self < py::self)
    .def(py::self <= py::self)
    .def(py::self > py::self)
    .def(py::self >= py::self)
    .def(py::self == py::self)
    .def(py::self != py::self);

  py::class_<set>(m, "Set")
    .def(py::init<>())
    .def("__len__", &set::size)
    .def("__iter__", [](const set &s) {
           return py::make_iterator(s.begin(), s.end());
         },
         py::keep_alive<0, 1>())
    .def("__contains__",
         [](const set& s, const data& d) {
           return s.count(d) == 1;
         })
    .def(py::self < py::self)
    .def(py::self <= py::self)
    .def(py::self > py::self)
    .def(py::self >= py::self)
    .def(py::self == py::self)
    .def(py::self != py::self);

  py::class_<table>(m, "Table")
    .def(py::init<>())
    .def("__len__", &table::size)
    .def("__iter__", [](const table &t) {
           return py::make_iterator(t.begin(), t.end());
         },
         py::keep_alive<0, 1>())
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
         py::return_value_policy::reference_internal);

  py::class_<configuration>(m, "Configuration")
    .def(py::init<>())
    // TODO: add ctor that takes command line arguments.
    ;

  py::class_<context>(m, "Context")
    .def("__init__",
         [](context& instance) {
           new (&instance) context{};
         })
    .def("__init__",
         [](context& instance, configuration& cfg) {
           new (&instance) context{std::move(cfg)};
         })
    .def("spawn_blocking", &context::spawn<blocking>)
    .def("spawn_nonblocking", &context::spawn<nonblocking>);

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
         })
    .def("attach_clone",
         [](endpoint& ep, const std::string& name) {
           return ep.attach<clone>(name);
         });

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
    .def("mailbox", &blocking_endpoint::mailbox);

  py::class_<nonblocking_endpoint>(m, "NonBlockingEndpoint",
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

  //
  // Implicit conversions (must come after type definitions)
  //

  py::implicitly_convertible<boolean, data>();
  py::implicitly_convertible<integer, data>();
  py::implicitly_convertible<count, data>();
  py::implicitly_convertible<real, data>();
  py::implicitly_convertible<std::string, data>();
  py::implicitly_convertible<interval, data>();
  py::implicitly_convertible<timestamp, data>();
  py::implicitly_convertible<address, data>();
  py::implicitly_convertible<subnet, data>();
  py::implicitly_convertible<port, data>();
  py::implicitly_convertible<vector, data>();
  py::implicitly_convertible<set, data>();
  py::implicitly_convertible<table, data>();


  py::implicitly_convertible<py::list, vector>();
  py::implicitly_convertible<py::dict, table>();

  py::implicitly_convertible<std::string, topic>();

  return m.ptr();
}
