
#include <pybind11/functional.h>
#include <pybind11/operators.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl_bind.h>

#include "broker/broker.hh"

namespace py = pybind11;

extern void init_data(py::module& m);
extern void init_enums(py::module& m);

PYBIND11_PLUGIN(_broker) {
  py::module m{"_broker", "Broker python bindings"};

  init_enums(m);
  init_data(m);

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

