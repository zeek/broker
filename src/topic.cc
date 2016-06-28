#include "broker/topic.hh"

namespace broker {

const std::string& topic::string() const {
  return str_;
}

bool operator==(const topic& lhs, const topic& rhs) {
  return lhs.string() == rhs.string();
}

bool operator<(const topic& lhs, const topic& rhs) {
  return lhs.string() < rhs.string();
}

bool convert(const topic& t, std::string& str) {
  str = t.string();
  return true;
}

} // namespace broker

broker::topic operator "" _t(const char* str, size_t) {
  return str;
}
