#include "broker/variant.hh"

#include "broker/detail/allocator.hh"
#include "broker/detail/assert.hh"
#include "broker/detail/type_traits.hh"
#include "broker/error.hh"
#include "broker/format/txt.hh"
#include "broker/variant_list.hh"
#include "broker/variant_set.hh"
#include "broker/variant_table.hh"

#include <functional>
#include <iterator>
#include <memory>
#include <memory_resource>
#include <type_traits>

namespace broker::detail {

namespace {

template <class T>
using mbr_allocator = broker::detail::allocator<T>;

using const_byte_pointer = const std::byte*;

} // namespace

namespace {

template <class Container>
struct container_storage {
  std::pmr::monotonic_buffer_resource resource;
  Container container;
  container_storage()
    : container(typename Container::allocator_type{&resource}) {}
};

const variant_data::set* empty_set_instance() {
  static container_storage<variant_data::set> instance;
  return &instance.container;
}

const variant_data::table* empty_table_instance() {
  static container_storage<variant_data::table> instance;
  return &instance.container;
}

const variant_data::list* empty_vector_instance() {
  static container_storage<variant_data::list> instance;
  return &instance.container;
}

} // namespace

} // namespace broker::detail

namespace broker {

bool variant::is_root() const noexcept {
  return envelope_ && envelope_->is_root(raw_);
}

data variant::to_data() const {
  return raw_->to_data();
}

variant_set variant::to_set() const noexcept {
  using detail_t = variant_data::set*;
  if (auto ptr = std::get_if<detail_t>(&stl_value()))
    return variant_set{*ptr, envelope_};
  return variant_set{detail::empty_set_instance(), nullptr};
}

variant_table variant::to_table() const noexcept {
  using detail_t = variant_data::table*;
  if (auto ptr = std::get_if<detail_t>(&stl_value()))
    return variant_table{*ptr, envelope_};
  return variant_table{detail::empty_table_instance(), nullptr};
}

variant_list variant::to_list() const noexcept {
  using detail_t = variant_data::list*;
  if (auto ptr = std::get_if<detail_t>(&stl_value()))
    return variant_list{*ptr, envelope_};
  return variant_list{detail::empty_vector_instance(), nullptr};
}

variant_list variant::to_vector() const noexcept {
  return to_list();
}

void convert(const variant& value, std::string& out) {
  format::txt::v1::encode(value, std::back_inserter(out));
}

std::ostream& operator<<(std::ostream& out, const variant& what) {
  format::txt::v1::encode(what, std::ostream_iterator<char>(out));
  return out;
}

} // namespace broker
