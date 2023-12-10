
#include <stdexcept>
#include <string>
#include <utility>

#ifdef __GNUC__
#  pragma GCC diagnostic push
#  pragma GCC diagnostic ignored "-Wpedantic"
#endif
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#ifdef __GNUC__
#  pragma GCC diagnostic pop
#endif

#include "broker/data.hh"
#include "broker/zeek.hh"

namespace py = pybind11;
using namespace pybind11::literals;

void init_zeek(py::module& m) {
  py::class_<broker::zeek::Message>(m, "Message")
    .def("as_data",
         [](const broker::zeek::Message& msg) { return msg.as_data(); });
  py::class_<broker::zeek::Event, broker::zeek::Message>(m, "Event")
    .def(py::init([](const broker::data& content) {
      auto topic_str = broker::topic::reserved;
      auto msg = broker::make_data_message(broker::topic{topic_str}, content);
      return broker::zeek::Event(msg->value());
    }))
    .def(py::init([](const std::string& name, const broker::data& args,
                     const std::optional<broker::data>& metadata) {
           const auto& args_vec = broker::get<broker::vector>(args);
           if (!metadata)
             return broker::zeek::Event(name, args_vec);
           return broker::zeek::Event(name, args_vec,
                                      broker::get<broker::vector>(*metadata));
         }),
         py::arg("name"), py::arg("args"), py::arg("metadata") = py::none())
    .def("valid",
         [](const broker::zeek::Event& ev) -> bool {
           auto t = broker::zeek::Message::type(ev.as_data());
           if (t != broker::zeek::Message::Type::Event)
             return false;
           return ev.valid();
         })
    .def("name",
         [](const broker::zeek::Event& ev) -> std::string {
           auto t = broker::zeek::Message::type(ev.as_data());
           if (t != broker::zeek::Message::Type::Event) {
             throw std::invalid_argument("invalid Event data/type");
           }
           if (!ev.valid()) {
             throw std::invalid_argument("invalid Event data");
           }
           return std::string{ev.name()};
         })
    .def("metadata",
         [](const broker::zeek::Event& ev) -> std::optional<broker::vector> {
           auto t = broker::zeek::Message::type(ev.as_data());
           if (t != broker::zeek::Message::Type::Event) {
             throw std::invalid_argument("invalid Event data/type");
           }
           if (!ev.valid()) {
             throw std::invalid_argument("invalid Event data");
           }
           if (auto meta = ev.metadata().raw(); !meta.empty()) {
             broker::vector result;
             convert(meta, result);
             return result;
           }

           return std::nullopt;
         })
    .def("args", [](const broker::zeek::Event& ev) -> broker::vector {
      auto t = broker::zeek::Message::type(ev.as_data());
      if (t != broker::zeek::Message::Type::Event) {
        throw std::invalid_argument("invalid Event data/type");
      }
      if (!ev.valid()) {
        throw std::invalid_argument("invalid Event data");
      }
      auto args = ev.args();
      broker::vector result;
      convert(args, result);
      return result;
    });
}
