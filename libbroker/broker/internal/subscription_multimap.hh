#pragma once

#include <algorithm>
#include <set>
#include <string>
#include <string_view>
#include <vector>

namespace broker::internal {

/// A multimap implementation that allows selecting values by prefix matching.
/// The keys are always strings, since we assume that keys represent topics.
template <class ValueType>
class subscription_multimap {
public:
  /// The type for representing keys.
  using key_type = std::string;

  /// The type for representing values.
  using value_type = ValueType;

  /// Stores a set of values for a given key.
  using value_set = std::set<value_type>;

  /// Inserts a value into the multimap.
  /// @param key The key to insert the value into.
  /// @param value The value to insert.
  /// @return True if the value was inserted, false if it was already present.
  bool insert(std::string_view key, const value_type& value) {
    auto i = std::find_if(entries_.begin(), entries_.end(),
                          [&key](const auto& e) { return e.first == key; });
    if (i == entries_.end()) {
      entries_.emplace_back(key, value_set{value});
      return true;
    }
    return i->second.insert(value).second;
  }

  /// Removes all entries that contain the given value.
  /// @param value The value to remove.
  /// @return The number of entries removed.
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

  /// Returns the values for the given key.
  /// @param key The key to get the values for.
  /// @return The values for the given key.
  const value_set& at(std::string_view key) const noexcept {
    auto i = std::find_if(entries_.begin(), entries_.end(),
                          [&key](const auto& e) { return e.first == key; });
    if (i != entries_.end()) {
      return i->second;
    }
    return empty_set_;
  }

  /// Checks if the multimap contains any values for the given key.
  /// @param key The key to check.
  /// @return True if the multimap contains any values for the given key, false
  ///         otherwise.
  bool contains(std::string_view key) const {
    return std::any_of(entries_.begin(), entries_.end(),
                       [&key](const auto& e) { return e.first == key; });
  }

  /// Appends all values from entries that have a key that is a prefix of `what`
  /// to `result`.
  /// @param what The string to match the keys against.
  /// @param result The output parameter to append the values to.
  void select(std::string_view what, std::vector<value_type>& result) const {
    for (const auto& [key, values] : entries_) {
      if (what.starts_with(key)) {
        result.insert(result.end(), values.begin(), values.end());
      }
    }
    std::sort(result.begin(), result.end());
    result.erase(std::unique(result.begin(), result.end()), result.end());
  }

  /// Appends all values from entries that have a key that is a prefix of `what`
  /// to a new vector and returns it.
  /// @param what The string to match the keys against.
  /// @return A new vector containing all values from entries that have a key
  ///         that is a prefix of `what`.
  std::vector<value_type> select(std::string_view what) const {
    std::vector<value_type> result;
    select(what, result);
    return result;
  }

private:
  /// Holds a key and the set of values for that key.
  using entry_type = std::pair<key_type, value_set>;

  /// The entries of the multimap. Sorted by key.
  std::vector<entry_type> entries_;

  /// An empty set of values for keys that are not present in the multimap.
  value_set empty_set_;
};

} // namespace broker::internal
