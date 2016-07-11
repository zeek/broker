#ifndef BROKER_MESSAGE_HH
#define BROKER_MESSAGE_HH

#include <tuple>

#include <caf/make_message.hpp>
#include <caf/message.hpp>

#include "broker/data.hh"

#include "broker/detail/type_traits.hh"

namespace broker {

/// A container for data.
using caf::message;

/// Constructs a message.
template <class T, class... Ts>
auto make_data_message(T&& x, Ts&&... xs)
-> detail::disable_if_t<
  std::is_same<message, detail::decay_t<T>>::value && sizeof...(Ts) == 0,
  message
> {
  static const auto factory = caf::message_factory{};
  auto args = std::make_tuple(data(std::forward<T>(x)),
                              data(std::forward<Ts>(xs))...);
  auto indices = caf::detail::get_indices(args);
  return caf::detail::apply_moved_args(factory, indices, args);
}

inline message make_data_message(message other) {
  return other;
}

} // namespace broker

#endif // BROKER_MESSAGE_HH
