#include "broker/error.hh"

#include <string_view>

#include "broker/data.hh"
#include "broker/detail/assert.hh"
#include "broker/endpoint_info.hh"

using namespace std::string_literals;

namespace broker {

namespace {

using namespace std::literals::string_view_literals;

constexpr auto ec_strings = std::array{
  "none"sv,
  "unspecified"sv,
  "peer_incompatible"sv,
  "peer_invalid"sv,
  "peer_unavailable"sv,
  "peer_disconnect_during_handshake"sv,
  "peer_timeout"sv,
  "master_exists"sv,
  "no_such_master"sv,
  "no_such_key"sv,
  "request_timeout"sv,
  "type_clash"sv,
  "invalid_data"sv,
  "backend_failure"sv,
  "stale_data"sv,
  "cannot_open_file"sv,
  "cannot_write_file"sv,
  "invalid_topic_key"sv,
  "end_of_file"sv,
  "invalid_tag"sv,
  "invalid_status"sv,
};

using ec_converter_t = detail::enum_converter<ec, decltype(ec_strings)>;

constexpr ec_converter_t ec_converter = ec_converter_t{&ec_strings};

} // namespace

bool convert(ec src, ec_ut& dst) noexcept {
  return ec_converter(src, dst);
}

bool convert(ec src, std::string& dst) {
  return ec_converter(src, dst);
}

bool convert(ec_ut src, ec& dst) noexcept {
  return ec_converter(src, dst);
}

bool convert(const std::string& src, ec& dst) noexcept {
  return ec_converter(src, dst);
}

bool convert(const data& src, ec& dst) noexcept {
  if (auto val = get_if<enum_value>(src))
    return convert(val->name, dst);
  else
    return false;
}

bool convertible_to_ec(const data& src) noexcept {
  ec dummy;
  return convert(src, dummy);
}

std::string to_string(ec code) {
  std::string result;
  convert(code, result);
  return result;
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
