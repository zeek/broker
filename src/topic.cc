#include "broker/topic.hh"

#include <caf/string_view.hpp>

namespace broker {

constexpr char topic::reserved[];

std::vector<std::string> topic::split(const topic& t) {
  std::vector<std::string> result;
  std::string::size_type i = 0;
  while (i != std::string::npos) {
    auto j = t.str_.find(sep, i);
    if (j == i) {
      ++i;
      continue;
    }
    if (j == std::string::npos)  {
      result.push_back(t.str_.substr(i));
      break;
    }
    result.push_back(t.str_.substr(i, j - i));
    i = (j == t.str_.size() - 1) ? std::string::npos : j + 1;
  }
  return result;
}

topic topic::join(const std::vector<std::string>& components) {
  topic result;
  for (auto& component : components)
    result /= component;
  return result;
}

topic& topic::operator/=(const topic& rhs) {
  if (!rhs.str_.empty() && rhs.str_[0] != sep && !str_.empty())
    str_ += sep;
  str_ += rhs.str_;
  if (!str_.empty() && str_.back() == sep)
    str_.pop_back();
  return *this;
}

const std::string& topic::string() const {
  return str_;
}

bool topic::prefix_of(const topic& t) const {
  return str_.size() <= t.str_.size()
         && t.str_.compare(0, str_.size(), str_) == 0;
}

void topic::clean() {
  // Remove one or more separators at the end.
  while (!str_.empty() && str_.back() == sep)
    str_.pop_back();
  // Replace multiple consecutive separators with a single one.
  static char sep2[] = {sep, sep};
  auto i = str_.find(sep2, 0, sizeof(sep2));
  if (i != std::string::npos) {
    auto j = str_.find_first_not_of(sep, i);
    str_.replace(i, j - i, 1, sep);
  }
}

bool operator==(const topic& lhs, const topic& rhs) {
  return lhs.string() == rhs.string();
}

bool operator<(const topic& lhs, const topic& rhs) {
  return lhs.string() < rhs.string();
}

topic operator/(const topic& lhs, const topic& rhs) {
  topic result{lhs};
  return result /= rhs;
}

bool convert(const topic& t, std::string& str) {
  str = t.string();
  return true;
}

namespace {

constexpr caf::string_view internal_prefix = "<$>/local/";

} // namespace

bool is_internal(const topic& x) {
  const auto& str = x.string();
  auto pre = internal_prefix;
  return str.size() >= pre.size()
         && caf::string_view{str.data(), pre.size()} == pre;
}

} // namespace broker

broker::topic operator "" _t(const char* str, size_t) {
  return str;
}
