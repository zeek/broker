#pragma once

#include "broker/envelope.hh"

namespace broker {

/// Wraps a value of type @ref variant and associates it with a @ref topic.
class data_envelope : public envelope {
public:
  envelope_type type() const noexcept final;

  envelope_ptr with(endpoint_id new_sender,
                    endpoint_id new_receiver) const final;

  std::string stringify() const override;

  /// Returns the contained value.
  /// @pre `root != nullptr`
  virtual variant value() const noexcept = 0;

  /// Checks whether `val` is the root value.
  virtual bool is_root(const variant_data* val) const noexcept = 0;

  /// Creates a new data envelope from the given @ref topic and @ref data.
  static data_envelope_ptr make(broker::topic t, const data& d);

  /// Creates a new data envelope from the given @ref topic and @ref data.
  static data_envelope_ptr make(const endpoint_id& sender,
                                const endpoint_id& receiver, broker::topic t,
                                const data& d);

  /// Creates a new data envelope from the given @ref topic and @ref data.
  static data_envelope_ptr make(broker::topic t, variant d);

  /// Creates a new data envelope from the given @ref topic and @ref data.
  static data_envelope_ptr make(std::string_view t, variant d);

  /// Attempts to deserialize an envelope from the given message in Broker's
  /// write format.
  static expected<data_envelope_ptr>
  deserialize(const endpoint_id& sender, const endpoint_id& receiver,
              uint16_t ttl, std::string_view topic_str,
              const std::byte* payload, size_t payload_size);

protected:
  /// Parses the data returned from @ref raw_bytes.
  variant_data* do_parse(detail::monotonic_buffer_resource& buf, error& err);
};

/// A shared pointer to a @ref data_envelope.
/// @relates data_envelope
using data_envelope_ptr = intrusive_ptr<const data_envelope>;

} // namespace broker
