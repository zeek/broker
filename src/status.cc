#include "broker/status.hh"

#include "broker/data.hh"
#include "broker/detail/assert.hh"

using namespace std::string_literals;

namespace broker {

const char* to_string(sc code) noexcept {
  switch (code) {
    default:
      BROKER_ASSERT(!"missing to_string implementation");
      return "<unknown>";
    case sc::unspecified:
      return "unspecified";
    case sc::peer_added:
      return "peer_added";
    case sc::peer_removed:
      return "peer_removed";
    case sc::peer_lost:
      return "peer_lost";
  }
}

#define BROKER_SC_FROM_STRING(value)                                           \
  if (str == #value) {                                                         \
    code = sc::value;                                                          \
    return true;                                                               \
  }

bool convert(const std::string& str, sc& code) noexcept {
  BROKER_SC_FROM_STRING(unspecified)
  BROKER_SC_FROM_STRING(peer_added)
  BROKER_SC_FROM_STRING(peer_removed)
  BROKER_SC_FROM_STRING(peer_lost)
  return false;
}

bool convert(const data& src, sc& code) noexcept {
  if (auto val = get_if<enum_value>(src))
    return convert(val->name, code);
  return false;
}

bool convertible_to_sc(const data& src) noexcept {
  sc dummy;
  return convert(src, dummy);
}

sc status::code() const {
  return code_;
}

const std::string* status::message() const {
  if (context_.empty())
    return nullptr;
  switch (code_) {
    default:
      return nullptr;
    case sc::unspecified:
      return context_.empty() ? nullptr : &context_.get_as<std::string>(0);
    case sc::peer_added:
    case sc::peer_removed:
    case sc::peer_lost:
      return &context_.get_as<std::string>(1);
  }
}

bool operator==(const status& x, const status& y) {
  return x.code_ == y.code_;
}

bool operator==(const status& x, sc y) {
  return x.code() == y;
}

bool operator==(sc x, const status& y) {
  return y == x;
}

std::string to_string(const status& s) {
  std::string result = to_string(s.code());
  if (!s.context_.empty())
    result += to_string(s.context_); // TODO: prettify
  return result;
}

bool convertible_to_status(const vector& xs) noexcept {
  if (!contains<std::string, sc, any_type>(xs))
    return false;
  if (get<std::string>(xs[0]) != "status")
    return false;
  // Context is always optional.
  if (is<none>(xs[2]))
    return true;
  // An `unspecified` status may only contain a string.
  if (get_as<sc>(xs[1]) == sc::unspecified)
    return contains<none, std::string>(xs[2]);
  // All other status codes contain an endpoint_info plus string.
  return contains<endpoint_info, std::string>(xs[2]);
}

bool convertible_to_status(const data& src) noexcept {
  if (auto xs = get_if<vector>(src))
    return convertible_to_status(*xs);
  return false;
}

bool convert(const data& src, status& dst) {
  if (!convertible_to_status(src))
    return false;
  auto& xs = get<vector>(src);
  sc code;
  convert(get<enum_value>(xs[1]).name, code);
  if (code == sc::unspecified) {
    if (is<none>(xs[2])) {
      dst.code_ = code;
      dst.context_ = caf::make_message();
      return true;
    }
    auto& ctx = get<vector>(xs[2]);
    dst.code_ = code;
    dst.context_ = caf::make_message(get<std::string>(ctx[1]));
    return true;
  }
  auto& ctx = get<vector>(xs[2]);
  endpoint_info ei;
  convert(ctx[0], ei);
  dst.code_ = code;
  dst.context_ = caf::make_message(std::move(ei), get<std::string>(ctx[1]));
  return true;
}

bool convert(const status& src, data& dst) {
  vector result;
  result.resize(3);
  result[0] = "status"s;
  result[1] = enum_value{to_string(src.code_)};
  if (src.context_.match_elements<endpoint_info, std::string>()) {
    result[2] = vector{};
    auto& context = get<vector>(result[2]);
    context.resize(2);
    if (!convert(src.context_.get_as<endpoint_info>(0), context[0]))
      return false;
    context[1] = src.context_.get_as<std::string>(1);
  } else if (src.context_.match_elements<std::string>()) {
    result[2] = vector{};
    auto& context = get<vector>(result[2]);
    context.resize(2);
    context[1] = src.context_.get_as<std::string>(0);
  }
  dst = std::move(result);
  return true;
}

sc status_view::code() const noexcept {
  BROKER_ASSERT(xs_ != nullptr);
  return get_as<sc>((*xs_)[1]);
}

const std::string* status_view::message() const noexcept {
  BROKER_ASSERT(xs_ != nullptr);
  if (is<none>((*xs_)[2]))
    return nullptr;
  auto& ctx = get<vector>((*xs_)[2]);
  return &get<std::string>(ctx[1]);
}

optional<endpoint_info> status_view::context() const {
  BROKER_ASSERT(xs_ != nullptr);
  if (is<none>((*xs_)[2]))
    return nil;
  auto& ctx = get<vector>((*xs_)[2]);
  if (is<none>(ctx[1]))
    return nil;
  endpoint_info ei;
  if (!convert(ctx[0], ei))
    return nil;
  return {std::move(ei)};
}

status_view status_view::make(const data& src) {
  return status_view{convertible_to_status(src) ? &get<vector>(src) : nullptr};
}

} // namespace broker
