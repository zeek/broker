#ifndef BROKER_TOPIC_HH
#define BROKER_TOPIC_HH

#include <string>
#include <type_traits>

#include "broker/detail/operators.hh"

namespace broker {

/// A hierachical topic used as pub/sub communication pattern.
class topic : detail::totally_ordered<topic> {
public:
  static constexpr char sep[] = "/";

  /// Default-constructs an empty topic.
  topic() = default;

  /// Constructs a topic from a type that is convertible to a string.
  /// @param x A value convertible to a string.
  template <
    class T,
    class = typename std::enable_if<
      std::is_convertible<T, std::string>::value
    >::type
  >
  topic(T&& x) : str_(std::forward<T>(x)) {
  }

  /// Retrieves the underlying string representation of the topic.
  const std::string& string() const;

private:
  std::string str_;
};

/// @relates topic
bool operator==(const topic& lhs, const topic& rhs);

/// @relates topic
bool operator<(const topic& lhs, const topic& rhs);

/// @relates topic
bool convert(const topic& t, std::string& str);

/// @relates topic
template <class Processor>
void serialize(Processor& proc, topic& t) {
  proc & t;
}

} // namespace broker

/// Converts a string to a topic.
/// @param str The string to convert.
/// @returns The topic according to *str*.
broker::topic operator "" _t(const char* str, size_t);

namespace std {

template <>
struct hash<broker::topic> {
  size_t operator()(const broker::topic& t) const {
    return std::hash<std::string>{}(t.string());
  }
};

} // namespace std

#endif // BROKER_TOPIC_HH
