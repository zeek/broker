
#include <pybind11/pybind11.h>

#include "broker/broker.hh"
#include "broker/bro.hh"

namespace py = pybind11;
using namespace pybind11::literals;

void init_bro(py::module& m) {
  py::class_<broker::bro::Message>(m, "Message")
    .def("as_data", &broker::bro::Message::as_data);

  py::class_<broker::bro::Event, broker::bro::Message>(m, "Event")
    .def("__init__",
       [](broker::bro::Event& ev, broker::data data) {
       new (&ev) broker::bro::Event(std::move(data));
       })
    .def("__init__",
       [](broker::bro::Event& ev, std::string name, broker::data args) {
       new (&ev) broker::bro::Event(std::move(name), std::move(broker::get<broker::vector>(args)));
       })
    .def("name",
          static_cast<const std::string& (broker::bro::Event::*)() const>
          (&broker::bro::Event::name))
    .def("args",
         static_cast<const broker::vector& (broker::bro::Event::*)() const>
         (&broker::bro::Event::args));
}

