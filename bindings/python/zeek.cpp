
#include <utility>
#include <string>
#include <stdexcept>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpedantic"
#include <pybind11/pybind11.h>
#pragma GCC diagnostic pop

#include "broker/zeek.hh"
#include "broker/data.hh"

namespace py = pybind11;
using namespace pybind11::literals;

void init_zeek(py::module& m) {
  py::class_<broker::zeek::Message>(m, "Message")
    .def("as_data",
         static_cast<const broker::data& (broker::zeek::Message::*)() const>
         (&broker::zeek::Message::as_data));

  py::class_<broker::zeek::Event, broker::zeek::Message>(m, "Event")
    .def(py::init([](broker::data data) {
       return broker::zeek::Event(std::move(data));
       }))
    .def(py::init([](std::string name, broker::data args) {
       return broker::zeek::Event(std::move(name), std::move(caf::get<broker::vector>(args)));
       }))
    .def("valid", [](const broker::zeek::Event& ev) -> bool {
         auto t = broker::zeek::Message::type(ev.as_data());
         if ( t != broker::zeek::Message::Type::Event )
           return false;
         return ev.valid();
         })
    .def("name", [](const broker::zeek::Event& ev) -> const std::string& {
         auto t = broker::zeek::Message::type(ev.as_data());
         if ( t != broker::zeek::Message::Type::Event ) {
           throw std::invalid_argument("invalid Event data/type");
         }
         if ( ! ev.valid() ) {
           throw std::invalid_argument("invalid Event data");
         }
         return ev.name();
         })
    .def("args", [](const broker::zeek::Event& ev) -> const broker::vector& {
         auto t = broker::zeek::Message::type(ev.as_data());
         if ( t != broker::zeek::Message::Type::Event ) {
           throw std::invalid_argument("invalid Event data/type");
         }
         if ( ! ev.valid() ) {
           throw std::invalid_argument("invalid Event data");
         }
         return ev.args();
         })
    ;
}

