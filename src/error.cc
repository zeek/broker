#include "broker/error.hh"

#include "broker/data.hh"
#include "broker/detail/assert.hh"
#include "broker/endpoint_info.hh"

using namespace std::string_literals;

namespace broker {

namespace {

const char* ec_names[] = {
  "none",
  "unspecified",
  "peer_incompatible",
  "peer_invalid",
  "peer_unavailable",
  "peer_disconnect_during_handshake",
  "peer_timeout",
  "master_exists",
  "no_such_master",
  "no_such_key",
  "request_timeout",
  "type_clash",
  "invalid_data",
  "backend_failure",
  "stale_data",
  "cannot_open_file",
  "cannot_write_file",
  "invalid_topic_key",
  "end_of_file",
  "invalid_tag",
  "invalid_status",
};

} // namespace

std::string to_string(ec code) noexcept {
  auto index = static_cast<uint8_t>(code);
  BROKER_ASSERT(index < sizeof(ec_names));
  return ec_names[index];
}

bool convert(const std::string& str, ec& code) noexcept {
  auto predicate = [&](const char* cstr) { return cstr == str; };
  auto begin = std::begin(ec_names);
  auto end = std::end(ec_names);
  auto i = std::find_if(begin, end, predicate);
  if (i == begin || i == end)
    return false;
  code = static_cast<ec>(std::distance(begin, i));
  return true;
}

bool convert(const data& src, ec& code) noexcept {
  if (auto val = get_if<enum_value>(src))
    return convert(val->name, code);
  return false;
}

bool convertible_to_ec(const data& src) noexcept {
  ec dummy;
  return convert(src, dummy);
}

bool convertible_to_error(const vector& xs) noexcept {
  if (!contains<std::string, ec, any_type>(xs)) {
    // There is one special case: default errors with enum value "none" fail to
    // convert to ec but are still legal.
    if (contains<std::string, enum_value, none>(xs))
      return get<std::string>(xs[0]) == "error"
             && get<enum_value>(xs[1]).name == "none";
    return false;
  }
  if (get<std::string>(xs[0]) != "error")
    return false;
  return is<none>(xs[2]) || contains<std::string>(xs[2])
         || contains<endpoint_info, std::string>(xs[2]);
}

bool convertible_to_error(const data& src) noexcept {
  if (auto xs = get_if<vector>(src))
    return convertible_to_error(*xs);
  return false;
}

bool convert(const error& src, data& dst) {
  if (!src) {
    vector result;
    result.resize(3);
    result[0] = "error"s;
    result[1] = enum_value{"none"};
    dst = std::move(result);
    return true;
  }
#if CAF_VERSION < 1800
  if (src.category() != caf::atom("broker"))
    return false;
#else
  if (src.category() != caf::type_id_v<broker::ec>)
    return false;
#endif
  vector result;
  result.resize(3);
  result[0] = "error"s;
  result[1] = enum_value{to_string(static_cast<ec>(src.code()))};
  auto& context = src.context();
  if (context.empty()) {
    dst = std::move(result);
    return true;
  }
  if (context.match_elements<std::string>()) {
    result[2] = vector{context.get_as<std::string>(0)};
    dst = std::move(result);
    return true;
  }
  if (context.match_elements<endpoint_info, std::string>()) {
    vector xs;
    xs.resize(2);
    if (!convert(context.get_as<endpoint_info>(0), xs[0]))
      return false;
    xs[1] = context.get_as<std::string>(1);
    result[2] = std::move(xs);
    dst = std::move(result);
    return true;
  }
  return false;
}

bool convert(const data& src, error& dst) {
  if (!convertible_to_error(src))
    return false;
  auto& xs = get<vector>(src);
  if (get<enum_value>(xs[1]).name == "none") {
    dst = error{};
    return true;
  }
  if (is<none>(xs[2])) {
    dst = make_error(get_as<ec>(xs[1]));
    return true;
  }
  auto& cxt = get<vector>(xs[2]);
  if (contains<std::string>(cxt)) {
    dst = make_error(get_as<ec>(xs[1]), get<std::string>(cxt[0]));
  } else {
    BROKER_ASSERT((contains<endpoint_info, std::string>(cxt)));
    dst = make_error(get_as<ec>(xs[1]), get_as<endpoint_info>(cxt[0]),
                     get<std::string>(cxt[1]));
  }
  return true;
}

ec error_view::code() const noexcept {
  return get_as<ec>((*xs_)[1]);
}

const std::string* error_view::message() const noexcept {
  if (is<none>((*xs_)[2]))
    return nullptr;
  auto& ctx = get<vector>((*xs_)[2]);
  return ctx.size() == 1 ? &get<std::string>(ctx[0])
                         : &get<std::string>(ctx[1]);
}

optional<endpoint_info> error_view::context() const {
  if (is<none>((*xs_)[2]))
    return nil;
  auto& ctx = get<vector>((*xs_)[2]);
  if (ctx.size() == 2)
    return get_as<endpoint_info>(ctx[0]);
  return nil;
}

error_view error_view::make(const data& src) {
  return error_view{convertible_to_error(src) ? &get<vector>(src) : nullptr};
}

} // namespace broker
