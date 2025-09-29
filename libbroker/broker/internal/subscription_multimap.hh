#pragma once

#include <algorithm>
#include <set>
#include <string>
#include <vector>

namespace broker::internal {

template <class ValueType>
class subscription_multimap {
public:
  using key_type = std::string;

  using value_type = ValueType;

  using value_set = std::set<value_type>;

  bool insert(const key_type& key, const value_type& value) {
    auto i = std::find_if(entries_.begin(), entries_.end(),
                          [&key](const auto& e) { return e.first == key; });
    if (i == entries_.end()) {
      entries_.emplace_back(key, value_set{value});
      return true;
    }
    return i->second.insert(value).second;
  }

  size_t purge_value(const value_type& value) {
    size_t count = 0;
    for (auto i = entries_.begin(); i != entries_.end();) {
      auto& [key, values] = *i;
      if (values.erase(value) > 0) {
        ++count;
        if (values.empty()) {
          i = entries_.erase(i);
        } else {
          ++i;
        }
      } else {
        ++i;
      }
    }
    return count;
  }

  const value_set& at(const key_type& key) const noexcept {
    auto i = std::find_if(entries_.begin(), entries_.end(),
                          [&key](const auto& e) { return e.first == key; });
    if (i != entries_.end()) {
      return i->second;
    }
    return empty_set_;
  }

  bool contains(const key_type& key) const {
    return std::any_of(entries_.begin(), entries_.end(),
                       [&key](const auto& e) { return e.first == key; });
  }

  /// Selects all values that match the given topic, i.e., appends all values
  /// from entries that have a key that is a prefix of the given topic.
  template <class T>
  void select(std::string_view topic, std::vector<T>& result) const {
    for (const auto& [key, values] : entries_) {
      if (topic.starts_with(key)) {
        for (const auto& value : values) {
          result.emplace_back(value);
        }
      }
    }
    std::sort(result.begin(), result.end());
    result.erase(std::unique(result.begin(), result.end()), result.end());
  }

  std::vector<value_type> select(const key_type& topic) const {
    std::vector<value_type> result;
    select(topic, result);
    return result;
  }

private:
  using entry_type = std::pair<key_type, value_set>;

  std::vector<entry_type> entries_;

  value_set empty_set_;
};

} // namespace broker::internal
