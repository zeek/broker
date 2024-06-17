#include "broker/internal/json.hh"

#include "broker/format/bin.hh"
#include "broker/internal/type_id.hh"
#include "broker/variant.hh"
#include "broker/variant_list.hh"
#include "broker/variant_set.hh"
#include "broker/variant_table.hh"

#include <caf/json_array.hpp>
#include <caf/json_object.hpp>
#include <caf/json_value.hpp>
#include <caf/type_id.hpp>

namespace broker::internal {

namespace {

template <class OutIter>
bool to_binary_impl(const caf::json_object& obj, OutIter& out) {
  namespace bin_v1 = format::bin::v1;
  auto dtype = obj.value("@data-type").to_string();
  auto dval = obj.value("data");
  if (dtype == "none") {
    out = bin_v1::encode_with_tag(nil, out);
    return true;
  }
  if (dtype == "boolean") {
    if (!dval.is_bool())
      return false;
    out = bin_v1::encode_with_tag(dval.to_bool(), out);
    return true;
  }
  if (dtype == "count") {
    if (!dval.is_unsigned())
      return false;
    auto val = static_cast<count>(dval.to_unsigned());
    out = bin_v1::encode_with_tag(val, out);
    return true;
  }
  if (dtype == "integer") {
    if (!dval.is_integer())
      return false;
    auto val = static_cast<integer>(dval.to_integer());
    out = bin_v1::encode_with_tag(val, out);
    return true;
  }
  if (dtype == "real") {
    if (!dval.is_double())
      return false;
    auto val = static_cast<real>(dval.to_double());
    out = bin_v1::encode_with_tag(val, out);
    return true;
  }
  if (dtype == "string") {
    if (!dval.is_string())
      return false;
    auto val = dval.to_string();
    out = bin_v1::encode_with_tag(val, out);
    return true;
  }
  if (dtype == "address") {
    if (!dval.is_string())
      return false;
    auto val = dval.to_string();
    broker::address tmp;
    if (!convert(std::string{val}, tmp))
      throw std::runtime_error("failed to convert address");
    out = bin_v1::encode_with_tag(tmp, out);
    return true;
  }
  if (dtype == "subnet") {
    if (!dval.is_string())
      return false;
    auto val = dval.to_string();
    broker::subnet tmp;
    if (!convert(std::string{val}, tmp))
      return false;
    out = bin_v1::encode_with_tag(tmp, out);
    return true;
  }
  if (dtype == "port") {
    if (!dval.is_string())
      return false;
    auto val = dval.to_string();
    broker::port tmp;
    if (!convert(std::string{val}, tmp))
      return false;
    out = bin_v1::encode_with_tag(tmp, out);
    return true;
  }
  if (dtype == "timestamp") {
    if (!dval.is_string())
      return false;
    auto val = dval.to_string();
    broker::timestamp tmp;
    if (auto err = caf::detail::parse(val, tmp))
      return false;
    out = bin_v1::encode_with_tag(tmp, out);
    return true;
  }
  if (dtype == "timespan") {
    if (!dval.is_string())
      return false;
    auto val = dval.to_string();
    broker::timespan tmp;
    if (auto err = caf::detail::parse(val, tmp))
      return false;
    out = bin_v1::encode_with_tag(tmp, out);
    return true;
  }
  if (dtype == "enum-value") {
    if (!dval.is_string())
      return false;
    broker::enum_value tmp{std::string{dval.to_string()}};
    out = bin_v1::encode_with_tag(tmp, out);
    return true;
  }
  if (dtype == "vector") {
    if (!dval.is_array())
      return false;
    auto xs = dval.to_array();
    out = bin_v1::write_unsigned(data::type::list, out);
    out = bin_v1::write_varbyte(xs.size(), out);
    for (const auto& x : xs)
      if (!to_binary_impl(x.to_object(), out))
        return false;
    return true;
  }
  if (dtype == "set") {
    if (!dval.is_array())
      return false;
    auto xs = dval.to_array();
    out = bin_v1::write_unsigned(data::type::set, out);
    out = bin_v1::write_varbyte(xs.size(), out);
    for (const auto& x : xs)
      if (!to_binary_impl(x.to_object(), out))
        return false;
    return true;
  }
  if (dtype == "table") {
    if (!dval.is_array())
      return false;
    auto xs = dval.to_array();
    out = bin_v1::write_unsigned(data::type::table, out);
    out = bin_v1::write_varbyte(xs.size(), out);
    for (const auto& x : xs) {
      auto kvp = x.to_object();
      auto key = kvp.value("key").to_object();
      auto val = kvp.value("value").to_object();
      if (!to_binary_impl(key, out) || !to_binary_impl(val, out))
        return false;
    }
    return true;
  }
  return false;
}

} // namespace

error json::data_message_to_binary(const caf::json_object& obj,
                                   std::vector<std::byte>& buf) {
  auto out = std::back_inserter(buf);
  if (to_binary_impl(obj, out))
    return {};
  return ec::type_clash;
}

} // namespace broker::internal
