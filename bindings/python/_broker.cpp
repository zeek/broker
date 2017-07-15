
#include <pybind11/functional.h>
#include <pybind11/operators.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl_bind.h>

#include "broker/broker.hh"

namespace py = pybind11;

extern void init_data(py::module& m);
extern void init_enums(py::module& m);

PYBIND11_MAKE_OPAQUE(broker::set);
PYBIND11_MAKE_OPAQUE(broker::table);
PYBIND11_MAKE_OPAQUE(broker::vector);

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

  py::class_<broker::publisher>(m, "Publisher")
    .def("demand", &broker::publisher::demand)
    .def("buffered", &broker::publisher::buffered)
    .def("capacity", &broker::publisher::capacity)
    .def("free_capacity", &broker::publisher::free_capacity)
    .def("send_rate", &broker::publisher::send_rate)
    .def("fd", &broker::publisher::fd)
    .def("drop_all_on_destruction", &broker::publisher::drop_all_on_destruction)
    .def("publish", (void (broker::publisher::*)(broker::data d)) &broker::publisher::publish)
    .def("publish_batch",
       [](broker::publisher& p, std::vector<broker::data> xs) { p.publish(xs); });

  using subscriber_base = broker::subscriber_base<broker::subscriber::value_type>;

  py::bind_vector<std::vector<subscriber_base::value_type>>(m, "VectorPairTopicData");

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

  using event_subscriber_base = broker::subscriber_base<broker::event_subscriber::value_type>;

  py::bind_vector<std::vector<event_subscriber_base::value_type>>(m, "VectorEventSubscriberValueType");

  py::class_<event_subscriber_base>(m, "EventSubscriberBase")
    .def("get", (event_subscriber_base::value_type (event_subscriber_base::*)()) &event_subscriber_base::get)
    .def("get",
         [](event_subscriber_base& ep, double secs) -> broker::optional<event_subscriber_base::value_type> {
	   return ep.get(broker::to_duration(secs)); })
    .def("get",
         [](event_subscriber_base& ep, size_t num) -> std::vector<event_subscriber_base::value_type> {
	   return ep.get(num); })
    .def("get",
         [](event_subscriber_base& ep, size_t num, double secs) -> std::vector<event_subscriber_base::value_type> {
	   return ep.get(num, broker::to_duration(secs)); })
    .def("poll",
         [](event_subscriber_base& ep) -> std::vector<event_subscriber_base::value_type> {
	   return ep.poll(); })
    .def("available", &event_subscriber_base::available)
    .def("fd", &event_subscriber_base::fd);

  py::class_<broker::event_subscriber, event_subscriber_base> event_subscriber(m, "EventSubscriber");

  py::class_<broker::event_subscriber::value_type>(event_subscriber, "ValueType")
    .def("is_error",
         [](broker::event_subscriber::value_type& x) -> bool { return broker::is<broker::error>(x);})
    .def("is_status",
         [](broker::event_subscriber::value_type& x) -> bool { return broker::is<broker::status>(x);})
    .def("get_error",
         [](broker::event_subscriber::value_type& x) -> broker::error { return broker::get<broker::error>(x);})
    .def("get_status",
         [](broker::event_subscriber::value_type& x) -> broker::status { return broker::get<broker::status>(x);});

  py::class_<broker::endpoint>(m, "Endpoint")
    .def(py::init<>())
    // .def(py::init<broker::configuration>())
    .def("listen", &broker::endpoint::listen, py::arg("address"), py::arg("port") = 0)
    .def("peer",
         [](broker::endpoint& ep, std::string& addr, uint16_t port, double retry) -> bool {
	 return ep.peer(addr, port, std::chrono::seconds((int)retry));},
         py::arg("addr"), py::arg("port"), py::arg("retry") = 10.0
         )
    .def("peer_nosync",
         [](broker::endpoint& ep, std::string& addr, uint16_t port, double retry) {
	 ep.peer_nosync(addr, port, std::chrono::seconds((int)retry));},
         py::arg("addr"), py::arg("port"), py::arg("retry") = 10.0
         )
    .def("unpeer", &broker::endpoint::peer)
    .def("peers", &broker::endpoint::peers)
    .def("peer_subscriptions", &broker::endpoint::peer_subscriptions)
    .def("publish", (void (broker::endpoint::*)(broker::topic t, broker::data d)) &broker::endpoint::publish)
    .def("publish", (void (broker::endpoint::*)(const broker::endpoint_info& dst, broker::topic t, broker::data d)) &broker::endpoint::publish)
    .def("publish_batch",
       [](broker::endpoint& ep, std::vector<broker::endpoint::value_type> xs) { ep.publish(xs); })
    .def("make_publisher", &broker::endpoint::make_publisher)
    .def("make_subscriber", &broker::endpoint::make_subscriber, py::arg("topics"), py::arg("max_qsize") = 20)
    .def("make_event_subscriber", &broker::endpoint::make_event_subscriber, py::arg("receive_statuses") = false)
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

