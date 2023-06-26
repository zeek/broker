
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
  namespace bz = broker::zeek;
  py::class_<bz::Message>(m, "Message")
    .def("as_data", [](const bz::Message& msg) -> broker::data {
      return msg.deep_copy();
    });
  py::class_<bz::Event, bz::Message>(m, "Event")
    .def(py::init([](const broker::data& data) {
      return bz::Event::convert_from(std::move(data));
    }))
    /*
      .def(py::init([](std::string name, broker::data args,
                       std::optional<broker::data> metadata) {
             if (metadata)
               return bz::Event(
                 std::move(name), std::move(broker::get<broker::vector>(args)),
                 std::move(broker::get<broker::vector>(*metadata)));
             else
               return bz::Event(
                 std::move(name), std::move(broker::get<broker::vector>(args)));
           }),
           py::arg("name"), py::arg("args"), py::arg("metadata") = py::none())
           */
    .def("valid",
         [](const bz::Event& ev) -> bool {
           auto t = bz::Message::type(ev.as_data());
           if (t != bz::Message::Type::Event)
             return false;
           return ev.valid();
         })
    .def("name",
         [](const bz::Event& ev) -> const std::string& {
           auto t = bz::Message::type(ev.as_data());
           if (t != bz::Message::Type::Event) {
             throw std::invalid_argument("invalid Event data/type");
           }
           if (!ev.valid()) {
             throw std::invalid_argument("invalid Event data");
           }
           return ev.name();
         })
    .def("metadata",
         [](const bz::Event& ev) -> std::optional<broker::vector> {
           auto t = bz::Message::type(ev.as_data());
           if (t != bz::Message::Type::Event) {
             throw std::invalid_argument("invalid Event data/type");
           }
           if (!ev.valid()) {
             throw std::invalid_argument("invalid Event data");
           }

           if (const auto vec = ev.metadata().get_vector())
             return *vec;

           return std::nullopt;
         })
    .def("args", [](const bz::Event& ev) -> const broker::vector& {
      auto t = bz::Message::type(ev.as_data());
      if (t != bz::Message::Type::Event) {
        throw std::invalid_argument("invalid Event data/type");
      }
      if (!ev.valid()) {
        throw std::invalid_argument("invalid Event data");
      }
      return ev.args();
    });
}
