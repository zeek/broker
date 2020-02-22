#include "broker/status.hh"

#include "broker/data.hh"
#include "broker/detail/assert.hh"

using namespace std::string_literals;

namespace broker {

const char* to_string(sc code) {
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

bool convert(const std::string& str, sc& code){
  BROKER_SC_FROM_STRING(unspecified)
  BROKER_SC_FROM_STRING(peer_added)
  BROKER_SC_FROM_STRING(peer_removed)
  BROKER_SC_FROM_STRING(peer_lost)
  return false;
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

bool convert(const data& src, status& dst) {
  if (!is<vector>(src))
    return false;
  auto& xs = get<vector>(src);
  if (xs.size() != 3)
    return false;
  if (!is<std::string>(xs[0]) || get<std::string>(xs[0]) != "status")
    return false;
  if (!is<enum_value>(xs[1]))
    return false;
  sc code;
  if (!convert(get<enum_value>(xs[1]).name, code))
    return false;
  if (code == sc::unspecified) {
    if (is<none>(xs[2])) {
      dst.code_ = code;
      dst.context_ = caf::make_message();
      return true;
    }
    if (is<vector>(xs[2])) {
      auto ctx = get<vector>(xs[2]);
      if (ctx.size() != 2)
        return false;
      if (!is<none>(ctx[0]))
        return false;
      if (!is<std::string>(ctx[1]))
        return false;
      dst.code_ = code;
      dst.context_ = caf::make_message(get<std::string>(ctx[1]));
      return true;
    }
  } else {
    if (!is<vector>(xs[2]))
      return false;
    auto ctx = get<vector>(xs[2]);
    if (ctx.size() != 2)
      return false;
    endpoint_info ei;
    if (!convert(ctx[0], ei))
      return false;
    if (!is<std::string>(ctx[1]))
      return false;
    dst.code_ = code;
    dst.context_ = caf::make_message(std::move(ei), get<std::string>(ctx[1]));
    return true;
  }
  return false;
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

} // namespace broker
