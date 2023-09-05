#include "broker/internal/json_type_mapper.hh"

#include "broker/internal/type_id.hh"

namespace broker::internal {

namespace {

struct tbl_entry {
  caf::type_id_t type;
  caf::string_view name;
};

constexpr tbl_entry tbl[] = {
  {caf::type_id_v<data_message>, "data-message"},
  {caf::type_id_v<none>, "none"},
  {caf::type_id_v<boolean>, "boolean"},
  {caf::type_id_v<count>, "count"},
  {caf::type_id_v<integer>, "integer"},
  {caf::type_id_v<real>, "real"},
  {caf::type_id_v<std::string>, "string"},
  {caf::type_id_v<address>, "address"},
  {caf::type_id_v<subnet>, "subnet"},
  {caf::type_id_v<port>, "port"},
  {caf::type_id_v<timestamp>, "timestamp"},
  {caf::type_id_v<timespan>, "timespan"},
  {caf::type_id_v<enum_value>, "enum-value"},
  {caf::type_id_v<set>, "set"},
  {caf::type_id_v<table>, "table"},
  {caf::type_id_v<vector>, "vector"},
};

} // namespace

caf::string_view json_type_mapper::operator()(caf::type_id_t type) const {
  for (auto& entry : tbl)
    if (entry.type == type)
      return entry.name;
  return caf::query_type_name(type);
}

caf::type_id_t json_type_mapper::operator()(caf::string_view name) const {
  for (auto& entry : tbl)
    if (entry.name == name)
      return entry.type;
  return caf::query_type_id(name);
}

} // namespace broker::internal
