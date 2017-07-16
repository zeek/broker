
#include <pybind11/pybind11.h>

#include "set_bind.h"

#include "broker/broker.hh"

namespace py = pybind11;
using namespace pybind11::literals;

void init_store(py::module& m) {

  py::class_<broker::optional<broker::timespan>>(m, "OptionalTimespan")
    .def(py::init<>())
    .def(py::init<broker::timespan>()
    );

  py::class_<broker::expected<broker::store>>(m, "ExpectedStore")
    .def("is_valid",
         [](broker::expected<broker::store>& e) -> bool { return static_cast<bool>(e);})
    .def("get",
         [](broker::expected<broker::store>& e) -> broker::store& { return *e; })
    ;

  py::class_<broker::expected<broker::data>>(m, "ExpectedData")
    .def("is_valid",
         [](broker::expected<broker::data>& e) -> bool { return static_cast<bool>(e);})
    .def("get",
         [](broker::expected<broker::data>& e) -> broker::data& { return *e; })
    ;

  py::class_<broker::store> store(m, "Store");
  store
    .def("name", &broker::store::name)
    .def("get", (broker::expected<broker::data> (broker::store::*)(broker::data d) const) &broker::store::get)
    .def("get", (broker::expected<broker::data> (broker::store::*)(broker::data d, broker::data aspect) const) &broker::store::get)
    .def("keys", &broker::store::keys)
    .def("put", &broker::store::put)
    .def("erase", &broker::store::erase)
    .def("clear", &broker::store::clear)
    .def("add", &broker::store::add)
    .def("subtract", &broker::store::subtract);

// Don't need.
//  py::class_<broker::store::response>(store, "Response")
//    .def_readwrite("answer", &broker::store::response::answer)
//    .def_readwrite("id", &broker::store::response::id);

}


