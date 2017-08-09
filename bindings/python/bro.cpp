
#include <pybind11/pybind11.h>

#include "broker/broker.hh"
#include "broker/bro.hh"

namespace py = pybind11;
using namespace pybind11::literals;

void init_bro(py::module& m) {

  py::class_<broker::bro::EventBase>(m, "EventBase")
    .def("as_data", &broker::bro::EventBase::as_data);

  py::class_<broker::bro::Event, broker::bro::EventBase>(m, "Event")
    .def(py::init<broker::data>())
    .def("__init__",
       [](broker::bro::Event& ev, std::string name, broker::data args) {
       new (&ev) broker::bro::Event(name, broker::get<broker::vector>(args));
       })
    .def("name", &broker::bro::Event::name)
    .def("args", &broker::bro::Event::args);
}

