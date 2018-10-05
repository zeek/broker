#include "broker/status.hh"

#include "broker/detail/assert.hh"

namespace broker {

const char* to_string(sc code) {
  switch (code) {
    default:
      BROKER_ASSERT(!"missing to_string implementation");
      return "<unknown>";
    case sc::unspecified:
      return "<unspecified>";
    case sc::peer_added:
      return "peer_added";
    case sc::peer_removed:
      return "peer_removed";
    case sc::peer_lost:
      return "peer_lost";
  }
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

} // namespace broker
