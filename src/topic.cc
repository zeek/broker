#include "broker/topic.hh"

#include <caf/string_view.hpp>

namespace broker {

std::vector<std::string> topic::split(const topic& t) {
  std::vector<std::string> result;
  std::string::size_type i = 0;
  while (i != std::string::npos) {
    auto j = t.str_.find(sep, i);
    if (j == i) {
      ++i;
      continue;
    }
    if (j == std::string::npos) {
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

std::string&& topic::move_string() && {
  return std::move(str_);
}

bool topic::prefix_of(const topic& t) const {
  return is_prefix(t, str_);
}

std::string_view topic::suffix() const noexcept {
  if (auto index = str_.find_last_of(sep); index != std::string::npos) {
    auto first = index + 1;
    return {str_.data() + first, str_.size() - first};
  } else {
    return {str_};
  }
}

bool is_prefix(const topic& t, std::string_view prefix) noexcept {
  const auto& str = t.string();
  return str.size() >= prefix.size()
         && str.compare(0, prefix.size(), prefix) == 0;
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

namespace {

constexpr caf::string_view internal_prefix = "<$>/local/";

} // namespace

bool is_internal(const topic& x) {
  const auto& str = x.string();
  auto pre = internal_prefix;
  return str.size() >= pre.size()
         && caf::string_view{str.data(), pre.size()} == pre;
}

topic topic::master_suffix() {
  return from(master_suffix_str);
}

topic topic::clone_suffix() {
  return from(clone_suffix_str);
}

topic topic::errors() {
  return from(errors_str);
}

topic topic::statuses() {
  return from(statuses_str);
}

topic topic::store_events() {
  return from(store_events_str);
}

} // namespace broker

broker::topic operator"" _t(const char* str, size_t len) {
  return broker::topic{std::string{str, len}};
}
