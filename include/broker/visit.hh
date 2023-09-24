#pragma once

#include "broker/variant.hh"
#include "broker/variant_list.hh"
#include "broker/variant_set.hh"
#include "broker/variant_table.hh"

namespace broker {

template <class Visitor>
decltype(auto) visit(Visitor&& visitor, const variant& x) {
  switch (x.get_tag()) {
    default:
      return visitor(nil);
    case variant_tag::boolean:
      return visitor(x.to_boolean());
    case variant_tag::count:
      return visitor(x.to_count());
    case variant_tag::integer:
      return visitor(x.to_integer());
    case variant_tag::real:
      return visitor(x.to_real());
    case variant_tag::string:
      return visitor(x.to_string());
    case variant_tag::address:
      return visitor(x.to_address());
    case variant_tag::subnet:
      return visitor(x.to_subnet());
    case variant_tag::port:
      return visitor(x.to_port());
    case variant_tag::timestamp:
      return visitor(x.to_timestamp());
    case variant_tag::timespan:
      return visitor(x.to_timespan());
    case variant_tag::enum_value:
      return visitor(x.to_enum_value());
    case variant_tag::set:
      return visitor(x.to_set());
    case variant_tag::table:
      return visitor(x.to_table());
    case variant_tag::list:
      return visitor(x.to_list());
  }
}

} // namespace broker
