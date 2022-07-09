
#include <string>
#include <utility>

#ifdef __GNUC__
#  pragma GCC diagnostic push
#  pragma GCC diagnostic ignored "-Wpedantic"
#endif
#include <pybind11/pybind11.h>
#ifdef __GNUC__
#  pragma GCC diagnostic pop
#endif

#include "broker/data.hh"
#include "broker/store.hh"

namespace py = pybind11;
using namespace pybind11::literals;

void init_store(py::module& m) {
  py::class_<std::optional<broker::timespan>>(m, "OptionalTimespan")
    .def(py::init<>())
    .def(py::init<broker::timespan>());

  py::class_<broker::expected<broker::store>>(m, "ExpectedStore")
    .def("is_valid",
         [](broker::expected<broker::store>& e) -> bool {
           return static_cast<bool>(e);
         })
    .def("get", [](broker::expected<broker::store>& e) -> broker::store& {
      return *e;
    });

  py::class_<broker::expected<broker::data>>(m, "ExpectedData")
    .def("is_valid",
         [](broker::expected<broker::data>& e) -> bool {
           return static_cast<bool>(e);
         })
    .def("get",
         [](broker::expected<broker::data>& e) -> broker::data& { return *e; });

  py::class_<broker::store> store(m, "Store");
  store.def("name", &broker::store::name)
    .def("exists", (broker::expected<broker::data>(broker::store::*)(
                     broker::data d) const)
                     & broker::store::exists)
    .def("get", (broker::expected<broker::data>(broker::store::*)(
                  broker::data d) const)
                  & broker::store::get)
    .def("get_index_from_value",
         (broker::expected<broker::data>(
           broker::store::*)(broker::data d, broker::data index) const)
           & broker::store::get_index_from_value)
    .def("keys", &broker::store::keys)
    .def("put", &broker::store::put)
    .def("put_unique", &broker::store::put_unique)
    .def("erase", &broker::store::erase)
    .def("clear", &broker::store::clear)
    .def("increment", &broker::store::increment)
    .def("decrement", &broker::store::decrement)
    .def("append", &broker::store::append)
    .def("insert_into",
         (void(broker::store::*)(broker::data, broker::data,
                                 std::optional<broker::timespan>))
           & broker::store::insert_into)
    .def("insert_into",
         (void(broker::store::*)(broker::data, broker::data, broker::data,
                                 std::optional<broker::timespan>))
           & broker::store::insert_into)
    .def("remove_from", &broker::store::remove_from)
    .def("push", &broker::store::push)
    .def("pop", &broker::store::pop)
    .def("await_idle", [](broker::store& st) { return st.await_idle(); })
    .def("await_idle",
         [](broker::store& st, broker::timespan timeout) {
           return st.await_idle(timeout);
         })
    .def("reset", &broker::store::reset);

  // Don't need.
  //  py::class_<broker::store::response>(store, "Response")
  //    .def_readwrite("answer", &broker::store::response::answer)
  //    .def_readwrite("id", &broker::store::response::id);
}
