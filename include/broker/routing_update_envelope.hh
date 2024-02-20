#pragma once

#include "broker/endpoint_id.hh"
#include "broker/envelope.hh"

#include <cstddef>
#include <cstdint>
#include <iterator>
#include <string_view>

namespace broker {

struct routing_update_sentinel {};

/// Lazily iterates a routing update message.
class routing_update_iterator {
public:
  using iterator_category = std::forward_iterator_tag;

  using value_type = std::string_view;

  using difference_type = ptrdiff_t;

  using pointer = void;

  constexpr routing_update_iterator() noexcept = default;

  constexpr routing_update_iterator(const std::byte* begin,
                                    const std::byte* end) noexcept
    : pos_(begin), end_(end) {
    // nop
  }

  constexpr routing_update_iterator(const routing_update_iterator&) noexcept =
    default;

  constexpr routing_update_iterator&
  operator=(const routing_update_iterator&) noexcept = default;

  std::string_view operator*() const;

  routing_update_iterator& operator++();

  constexpr const std::byte* ptr() const noexcept {
    return pos_;
  }

  constexpr bool at_end() const noexcept {
    return pos_ == end_;
  }

private:
  const std::byte* pos_ = nullptr;
  const std::byte* end_ = nullptr;
};

/// @relates routing_update_iterator
inline bool operator==(const routing_update_iterator& iter,
                       routing_update_sentinel) noexcept {
  return iter.at_end();
}

/// @relates routing_update_iterator
inline bool operator==(routing_update_sentinel,
                       const routing_update_iterator& iter) noexcept {
  return iter.at_end();
}

/// @relates routing_update_iterator
inline bool operator!=(routing_update_sentinel,
                       const routing_update_iterator& iter) noexcept {
  return !iter.at_end();
}

/// @relates routing_update_iterator
inline bool operator!=(const routing_update_iterator& iter,
                       routing_update_sentinel) noexcept {
  return !iter.at_end();
}

/// Wraps a filter update. The associated topic is always `topic::reserved`.
class routing_update_envelope : public envelope {
public:
  envelope_type type() const noexcept final;

  std::string_view topic() const noexcept override;

  envelope_ptr with(endpoint_id new_sender,
                    endpoint_id new_receiver) const override;

  std::string stringify() const override;

  /// Returns the number of entries in the filter.
  size_t filter_size() const noexcept;

  /// Returns an iterator to the first entry in the filter.
  routing_update_iterator begin() const noexcept;

  /// Returns the past-the-end iterator for the filter.
  routing_update_sentinel end() const noexcept {
    return {};
  }

  /// Creates a new routing_update envelope from the arguments.
  static routing_update_envelope_ptr
  make(const std::vector<broker::topic>& entries);

  /// Attempts to deserialize an envelope from the given message in Broker's
  /// write format.
  static expected<envelope_ptr>
  deserialize(const endpoint_id& sender, const endpoint_id& receiver,
              uint16_t ttl, std::string_view topic_str,
              const std::byte* payload, size_t payload_size);
};

/// A shared pointer to a @ref routing_update_envelope.
/// @relates routing_update_envelope
using routing_update_envelope_ptr =
  intrusive_ptr<const routing_update_envelope>;

} // namespace broker
