#include "broker/internal/json.hh"

#include "broker/format/bin.hh"
#include "broker/internal/type_id.hh"
#include "broker/variant.hh"
#include "broker/variant_list.hh"
#include "broker/variant_set.hh"
#include "broker/variant_table.hh"

#include <caf/json_array.hpp>
#include <caf/json_object.hpp>
#include <caf/json_reader.hpp>
#include <caf/json_value.hpp>
#include <caf/json_writer.hpp>
#include <caf/type_id.hpp>

namespace broker::internal {

namespace {

struct checker {};

constexpr checker check;

const checker& operator<<(const checker& check, bool value) {
  if (!value)
    throw std::logic_error("failed to generate JSON");
  return check;
}

bool do_apply(const variant& val, caf::json_writer& out);

bool do_apply(const variant_list& ls, caf::json_writer& out) {
  check << out.begin_sequence(ls.size());
  for (auto val : ls) {
    check << out.begin_object(caf::type_id_v<data>, "data") //
          << do_apply(val, out)                             //
          << out.end_object();
  }
  check << out.end_sequence();
  return true;
}

bool do_apply(const variant_set& ls, caf::json_writer& out) {
  check << out.begin_sequence(ls.size());
  for (auto val : ls) {
    check << out.begin_object(caf::type_id_v<data>, "data") //
          << do_apply(val, out)                             //
          << out.end_object();
  }
  check << out.end_sequence();
  return true;
}

bool do_apply(const variant_table& tbl, caf::json_writer& out) {
  using namespace std::literals;
  check << out.begin_sequence(tbl.size());
  for (const auto& [key, val] : tbl) {
    check << out.begin_associative_array(2)                 //
          << out.begin_key_value_pair()                     //
          << out.value("key"sv)                             //
          << out.begin_object(caf::type_id_v<data>, "data") //
          << do_apply(key, out)                             //
          << out.end_object()                               //
          << out.end_key_value_pair()                       //
          << out.begin_key_value_pair()                     //
          << out.value("value"sv)                           //
          << out.begin_object(caf::type_id_v<data>, "data") //
          << do_apply(val, out)                             //
          << out.end_object()                               //
          << out.end_key_value_pair()                       //
          << out.end_associative_array();
  }
  check << out.end_sequence();
  return true;
}

bool do_apply(const variant& val, caf::json_writer& out) {
  auto caf_print = [](auto val) {
    std::string result;
    caf::detail::print(result, val);
    return result;
  };
  check << out.begin_field("@data-type")       //
        << out.value(json_name(val.get_tag())) //
        << out.end_field()                     //
        << out.begin_field("data");
  switch (val.get_tag()) {
    default: // none
      check << out.begin_object(caf::type_id_v<none>, "none")
            << out.end_object();
      break;
    case variant_tag::boolean:
      check << out.value(val.to_boolean());
      break;
    case variant_tag::count:
      check << out.value(val.to_count());
      break;
    case variant_tag::integer:
      check << out.value(val.to_integer());
      break;
    case variant_tag::real:
      check << out.value(val.to_real());
      break;
    case variant_tag::string:
      check << out.value(val.to_string());
      break;
    case variant_tag::address:
      check << out.value(broker::to_string(val.to_address()));
      break;
    case variant_tag::subnet:
      check << out.value(broker::to_string(val.to_subnet()));
      break;
    case variant_tag::port:
      check << out.value(broker::to_string(val.to_port()));
      break;
    case variant_tag::timestamp:
      check << out.value(caf_print(val.to_timestamp()));
      break;
    case variant_tag::timespan:
      check << out.value(caf_print(val.to_timespan()));
      break;
    case variant_tag::enum_value:
      check << out.value(val.to_enum_value().name);
      break;
    case variant_tag::set:
      do_apply(val.to_set(), out);
      break;
    case variant_tag::table:
      do_apply(val.to_table(), out);
      break;
    case variant_tag::list:
      do_apply(val.to_list(), out);
      break;
  }
  check << out.end_field();
  return true;
}

bool do_apply(const data_message& msg, caf::json_writer& out) {
  using namespace std::literals;
  check << out.begin_object(caf::type_id_v<data_message>, "data-message") //
        << out.begin_field("type")                                        //
        << out.value("data-message"sv)                                    //
        << out.end_field()                                                //
        << out.begin_field("topic")                                       //
        << out.value(msg->topic())                                        //
        << out.end_field()                                                //
        << do_apply(msg->value(), out)                                    //
        << out.end_object();
  return true;
}

} // namespace

void json::apply(const data_message& msg, caf::json_writer& out) {
  do_apply(msg, out);
}

namespace {

template <class OutIter>
bool to_binary_impl(const caf::json_object& obj, OutIter& out) {
  auto dtype = obj.value("@data-type").to_string();
  auto dval = obj.value("data");
  if (dtype == "none") {
    out = format::bin::v1::encode(nil, out);
    return true;
  }
  if (dtype == "boolean") {
    if (!dval.is_bool())
      return false;
    out = format::bin::v1::encode(dval.to_bool(), out);
    return true;
  }
  if (dtype == "count") {
    if (!dval.is_unsigned())
      return false;
    auto val = static_cast<count>(dval.to_unsigned());
    out = format::bin::v1::encode(val, out);
    return true;
  }
  if (dtype == "integer") {
    if (!dval.is_integer())
      return false;
    auto val = static_cast<integer>(dval.to_integer());
    out = format::bin::v1::encode(val, out);
    return true;
  }
  if (dtype == "real") {
    if (!dval.is_double())
      return false;
    auto val = static_cast<real>(dval.to_double());
    out = format::bin::v1::encode(val, out);
    return true;
  }
  if (dtype == "string") {
    if (!dval.is_string())
      return false;
    auto val = caf::to_string(dval.to_string());
    out = format::bin::v1::encode(val, out);
    return true;
  }
  if (dtype == "address") {
    if (!dval.is_string())
      return false;
    auto val = dval.to_string();
    broker::address tmp;
    if (!convert(caf::to_string(val), tmp))
      throw std::runtime_error("failed to convert address");
    out = format::bin::v1::encode(tmp, out);
    return true;
  }
  if (dtype == "subnet") {
    if (!dval.is_string())
      return false;
    auto val = dval.to_string();
    broker::subnet tmp;
    if (!convert(caf::to_string(val), tmp))
      return false;
    out = format::bin::v1::encode(tmp, out);
    return true;
  }
  if (dtype == "port") {
    if (!dval.is_string())
      return false;
    auto val = dval.to_string();
    broker::port tmp;
    if (!convert(caf::to_string(val), tmp))
      return false;
    out = format::bin::v1::encode(tmp, out);
    return true;
  }
  if (dtype == "timestamp") {
    if (!dval.is_string())
      return false;
    auto val = dval.to_string();
    broker::timestamp tmp;
    if (auto err = caf::detail::parse(val, tmp))
      return false;
    out = format::bin::v1::encode(tmp, out);
    return true;
  }
  if (dtype == "timespan") {
    if (!dval.is_string())
      return false;
    auto val = dval.to_string();
    broker::timespan tmp;
    if (auto err = caf::detail::parse(val, tmp))
      return false;
    out = format::bin::v1::encode(tmp, out);
    return true;
  }
  if (dtype == "enum-value") {
    if (!dval.is_string())
      return false;
    broker::enum_value tmp{caf::to_string(dval.to_string())};
    out = format::bin::v1::encode(tmp, out);
    return true;
  }
  if (dtype == "vector") {
    if (!dval.is_array())
      return false;
    auto xs = dval.to_array();
    out = format::bin::v1::write_unsigned(data::type::list, out);
    out = format::bin::v1::write_varbyte(xs.size(), out);
    for (const auto& x : xs)
      if (!to_binary_impl(x.to_object(), out))
        return false;
    return true;
  }
  if (dtype == "set") {
    if (!dval.is_array())
      return false;
    auto xs = dval.to_array();
    out = format::bin::v1::write_unsigned(data::type::set, out);
    out = format::bin::v1::write_varbyte(xs.size(), out);
    for (const auto& x : xs)
      if (!to_binary_impl(x.to_object(), out))
        return false;
    return true;
  }
  if (dtype == "table") {
    if (!dval.is_array())
      return false;
    auto xs = dval.to_array();
    out = format::bin::v1::write_unsigned(data::type::table, out);
    out = format::bin::v1::write_varbyte(xs.size(), out);
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
