#include "broker/status.hh"

#include "broker/data.hh"
#include "broker/detail/assert.hh"
#include "broker/error.hh"

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
    case sc::endpoint_discovered:
      return "endpoint_discovered";
    case sc::endpoint_unreachable:
      return "endpoint_unreachable";
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
  BROKER_SC_FROM_STRING(endpoint_discovered)
  BROKER_SC_FROM_STRING(endpoint_unreachable)
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

caf::error status::verify() const {
  switch (code_) {
    default:
      return make_error(ec::invalid_status, "invalid enum value");
    case sc::unspecified:
      if (!context_.node && !context_.network)
        return nil;
      return make_error(ec::invalid_status,
                        "the unspecified status may not have any context");
    case sc::peer_added:
    case sc::peer_removed:
    case sc::peer_lost:
    case sc::endpoint_discovered:
    case sc::endpoint_unreachable:
      if (context_.node)
        return nil;
      return make_error(ec::invalid_status,
                        "a non-default status must provide a node ID");
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
  result += '(';
  if (s.context_.node) {
    result += to_string(s.context_.node);
    if (s.context_.network) {
      result += ", ";
      result += to_string(*s.context_.network);
    }
    result += ", ";
  }
  result += '"';
  result += to_string(s.message_);
  result += "\")";
  return result;
}

bool convertible_to_status(const vector& xs) noexcept {
  if (xs.size() != 4 || !is<std::string>(xs[0]))
    return false;
  if (get<std::string>(xs[0]) != "status")
    return false;
  if (auto code = to<sc>(xs[1]))
    return *code != sc::unspecified
             ? contains<any_type, any_type, endpoint_info, std::string>(xs)
             : contains<any_type, any_type, none, none>(xs);
  return false;
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
  if (!convert(get<enum_value>(xs[1]).name, dst.code_))
    return false;
  if (dst.code_ != sc::unspecified) {
    if (convert(get<vector>(xs[2]), dst.context_)) {
      dst.message_ = get<std::string>(xs[3]);
      return true;
    }
  } else {
    dst.context_ = endpoint_info{};
    dst.message_.clear();
    return true;
  }
  return false;
}

bool convert(const status& src, data& dst) {
  vector result;
  result.resize(4);
  result[0] = "status"s;
  result[1] = enum_value{to_string(src.code_)};
  if (src.code_ != sc::unspecified) {
    if (!convert(src.context_, result[2]))
      return false;
    result[3] = src.message_;
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
  if (is<none>((*xs_)[3]))
    return nullptr;
  return &get<std::string>((*xs_)[3]);
}

optional<endpoint_info> status_view::context() const {
  BROKER_ASSERT(xs_ != nullptr);
  endpoint_info ei;
  if (!convert((*xs_)[2], ei))
    return nil;
  return {std::move(ei)};
}

status_view status_view::make(const data& src) {
  return status_view{convertible_to_status(src) ? &get<vector>(src) : nullptr};
}

} // namespace broker
