#pragma once

#include "broker/variant.hh"
#include "broker/variant_list.hh"
#include "broker/variant_set.hh"
#include "broker/variant_table.hh"

namespace broker {

template <class Visitor>
auto visit(Visitor&& visitor, const variant& what) {
  switch (what.get_tag()) {
    case variant_tag::boolean:
      return visitor(std::get<bool>(what.stl_value()));
    case variant_tag::count:
      return visitor(std::get<count>(what.stl_value()));
    case variant_tag::integer:
      return visitor(std::get<integer>(what.stl_value()));
    case variant_tag::real:
      return visitor(std::get<real>(what.stl_value()));
    case variant_tag::string:
      return visitor(std::get<std::string_view>(what.stl_value()));
    case variant_tag::address:
      return visitor(std::get<address>(what.stl_value()));
    case variant_tag::subnet:
      return visitor(std::get<subnet>(what.stl_value()));
    case variant_tag::port:
      return visitor(std::get<port>(what.stl_value()));
    case variant_tag::timestamp:
      return visitor(std::get<timestamp>(what.stl_value()));
    case variant_tag::timespan:
      return visitor(std::get<timespan>(what.stl_value()));
    case variant_tag::enum_value:
      return visitor(std::get<enum_value_view>(what.stl_value()));
    case variant_tag::set:
      return visitor(what.to_set());
    case variant_tag::table:
      return visitor(what.to_table());
    case variant_tag::list:
      return visitor(what.to_list());
    default:
      return visitor(nil);
  }
}

} // namespace broker
