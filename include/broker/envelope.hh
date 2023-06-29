#pragma once

#include "broker/fwd.hh"

#include <memory>

namespace broker {

/// Wraps a value of type @ref variant and associates it with a @ref topic.
class envelope : public std::enable_shared_from_this<envelope> {
public:
  virtual ~envelope();

  /// Returns the contained value.
  /// @pre `root != nullptr`
  virtual variant value() const noexcept = 0;

  /// Returns the topic for the data in this envelope.
  virtual std::string_view topic() const noexcept = 0;

  /// Checks whether `val` is the root value.
  virtual bool is_root(const variant_data* val) const noexcept = 0;

  /// Returns the contained value in its serialized form, if available. If the
  /// envelope does not contain serialized data, returns `nullptr` and `0`.
  virtual std::pair<const std::byte*, size_t> raw_bytes() const noexcept = 0;

  /// Creates a new data envolope from the given @ref topic and @ref data.
  static envelope_ptr make(broker::topic t, const data& d);

  /// Creates a new data envolope from the given @ref topic and @ref data.
  static envelope_ptr make(broker::topic t, variant d);

protected:
  /// Parses the data returned from @ref raw_bytes.
  variant_data* do_parse(detail::monotonic_buffer_resource& buf, error& err);
};

/// A shared pointer to an @ref envelope.
using envelope_ptr = std::shared_ptr<const envelope>;

} // namespace broker
