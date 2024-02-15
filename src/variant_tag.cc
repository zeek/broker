#include "broker/variant_tag.hh"

namespace {

constexpr const char* cpp_type_names[] = {
  "none",     "boolean",    "count",  "integer", "real",
  "string",   "address",    "subnet", "port",    "timestamp",
  "timespan", "enum_value", "set",    "table",   "vector",
};

constexpr std::string_view json_type_names[] = {
  "none",     "boolean",    "count",  "integer", "real",
  "string",   "address",    "subnet", "port",    "timestamp",
  "timespan", "enum-value", "set",    "table",   "vector",
};

} // namespace

namespace broker::detail {

std::string_view json_type_name(variant_tag tag) {
  return json_type_names[static_cast<size_t>(tag)];
}

const char* cpp_type_name(variant_tag tag) {
  return cpp_type_names[static_cast<size_t>(tag)];
}

} // namespace broker::detail
