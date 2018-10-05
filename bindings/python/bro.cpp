
#include <utility>
#include <string>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpedantic"
#include <pybind11/pybind11.h>
#pragma GCC diagnostic pop

#include "broker/bro.hh"
#include "broker/data.hh"

namespace py = pybind11;
using namespace pybind11::literals;

void init_bro(py::module& m) {
  py::class_<broker::bro::Message>(m, "Message")
    .def("as_data", &broker::bro::Message::as_data);

  py::class_<broker::bro::Event, broker::bro::Message>(m, "Event")
    .def(py::init([](broker::data data) {
       return broker::bro::Event(std::move(data));
       }))
    .def(py::init([](std::string name, broker::data args) {
       return broker::bro::Event(std::move(name), std::move(caf::get<broker::vector>(args)));
       }))
    .def("name",
          static_cast<const std::string& (broker::bro::Event::*)() const>
          (&broker::bro::Event::name))
    .def("args",
         static_cast<const broker::vector& (broker::bro::Event::*)() const>
         (&broker::bro::Event::args));
}

