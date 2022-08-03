#pragma once

#include <cstddef>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include "broker/detail/operators.hh"

namespace broker {

/// A hierachical topic used as pub/sub communication pattern.
class topic : detail::totally_ordered<topic> {
public:
  /// The separator between topic hierarchies.
  static constexpr char sep = '/';

  /// A reserved string which must not appear in a user topic.
  static constexpr std::string_view reserved = "<$>";

  static constexpr std::string_view master_suffix_str = "<$>/data/master";

  static constexpr std::string_view clone_suffix_str = "<$>/data/clone";

  static constexpr std::string_view errors_str = "<$>/local/data/errors";

  static constexpr std::string_view statuses_str = "<$>/local/data/statuses";

  static constexpr std::string_view store_events_str =
    "<$>/local/data/store-events";

  static topic master_suffix();

  static topic clone_suffix();

  static topic errors();

  static topic statuses();

  static topic store_events();

  /// Splits a topic into a vector of its components.
  /// @param t The topic to split.
  /// @returns The components that make up the topic.
  static std::vector<std::string> split(const topic& t);

  /// Joins a sequence of components to a hierarchical topic.
  /// @param components The components that make up the topic.
  /// @returns The topic according to *components*.
  static topic join(const std::vector<std::string>& components);

  /// Default-constructs an empty topic.
  topic() = default;

  /// Constructs a topic from a type that is convertible to a string.
  /// @param x A value convertible to a string.
  template <class T,
            class = std::enable_if_t<std::is_convertible_v<T, std::string>>>
  topic(T&& x) : str_(std::forward<T>(x)) {
    // nop
  }

  /// Appends a topic components with a separator.
  /// @param t The topic to append to this instance.
  topic& operator/=(const topic& t);

  /// Retrieves the underlying string representation of the topic.
  /// @returns A reference to the underlying string.
  const std::string& string() const;

  /// Retrieves an rvalue reference to the underlying string representation.
  std::string&& move_string() &&;

  /// Returns whether this topic is a prefix match for `t`.
  bool prefix_of(const topic& t) const;

  /// Returns the suffix of the topic, i.e., the characters after the last
  /// separator. For example, the suffix of `/foo/bar` is `bar`.
  std::string_view suffix() const noexcept;

  /// Returns whether this topic was default-constructed.
  [[nodiscard]] bool empty() const noexcept {
    return str_.empty();
  }

  template <class Inspector>
  friend bool inspect(Inspector& f, topic& x) {
    return f.apply(x.str_);
  }

  friend bool operator==(const topic& lhs, std::string_view rhs) {
    return lhs.str_ == rhs;
  }

  friend bool operator==(std::string_view lhs, const topic& rhs) {
    return lhs == rhs.str_;
  }

private:
  static topic from(std::string_view str) {
    return topic{std::string{str}};
  }

  std::string str_;
};

/// Returns whether `prefix` is a prefix match for `t`.
bool is_prefix(const topic& t, std::string_view prefix) noexcept;

/// @relates topic
bool operator==(const topic& lhs, const topic& rhs);

/// @relates topic
bool operator<(const topic& lhs, const topic& rhs);

/// @relates topic
topic operator/(const topic& lhs, const topic& rhs);

/// @relates topic
inline void convert(const topic& t, std::string& str) {
  str = t.string();
}

/// Checks whether a topic is internal, i.e., messages on this topic are always
/// only visible locally and never forwarded to peers.
/// @relates topic
bool is_internal(const topic& x);

} // namespace broker

/// Converts a string to a topic.
/// @param str The string to convert.
/// @returns The topic according to *str*.
broker::topic operator"" _t(const char* str, size_t);

namespace std {

template <>
struct hash<broker::topic> {
  size_t operator()(const broker::topic& t) const {
    return std::hash<std::string>{}(t.string());
  }
};

} // namespace std
