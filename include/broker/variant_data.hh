#pragma once

#include "broker/address.hh"
#include "broker/detail/monotonic_buffer_resource.hh"
#include "broker/enum_value.hh"
#include "broker/fwd.hh"
#include "broker/none.hh"
#include "broker/port.hh"
#include "broker/subnet.hh"

#include "broker/detail/type_traits.hh"

#include <list>
#include <map>
#include <set>

namespace broker {

/// Holds the data of a @ref variant.
class variant_data {
public:
  // -- member types -----------------------------------------------------------

  /// Evaluates to `true` if `T` one of the primitive value types.
  template <class T>
  inline static constexpr bool is_primitive =
    detail::is_one_of_v<T, none, boolean, count, integer, real,
                        std::string_view, address, subnet, port, timestamp,
                        timespan, enum_value_view>;

  // Note: GCC-8 has a broken std::less<> implementation. Remove this workaround
  //       once we drop support for GCC-8.
  struct less {
    template <class T, class U>
    bool operator()(const T& x, const U& y) const {
      return x < y;
    }
  };

  template <class T>
  using allocator_t = detail::monotonic_buffer_resource::allocator<T>;

  using list = std::list<variant_data, allocator_t<variant_data>>;

  using list_iterator = list::const_iterator;

  using set = std::set<variant_data, less, allocator_t<variant_data>>;

  using set_iterator = set::const_iterator;

  using key_value_pair = std::pair<const variant_data, variant_data>;

  using table_allocator = allocator_t<key_value_pair>;

  using table = std::map<variant_data, variant_data, less, table_allocator>;

  using table_iterator = table::const_iterator;

  using stl_type =
    std::variant<none, boolean, count, integer, real, std::string_view, address,
                 subnet, port, timestamp, timespan, enum_value_view, set*,
                 table*, list*>;

  // -- static factories -------------------------------------------------------

  /// Singleton for empty data.
  static const variant_data* nil() noexcept;

  // -- properties -------------------------------------------------------------

  /// Returns the type of the contained data.
  variant_tag get_tag() const noexcept {
    return static_cast<variant_tag>(value.index());
  }

  // -- conversion -------------------------------------------------------------

  /// Converts this object to a @ref broker::data object. Performs a deep copy.
  data to_data() const;

  /// Returns a reference to the `std::variant` stored in this object.
  const auto& stl_value() const noexcept {
    return value;
  }

  // -- deserialization --------------------------------------------------------

  std::pair<bool, const std::byte*>
  parse_shallow(detail::monotonic_buffer_resource& buf, const std::byte* pos,
                const std::byte* end);

  std::pair<bool, const std::byte*>
  parse_shallow(detail::monotonic_buffer_resource& buf, const std::byte* bytes,
                size_t num_bytes) {
    return parse_shallow(buf, bytes, bytes + num_bytes);
  }

  // -- member variables -------------------------------------------------------

  stl_type value;
};

// -- free functions -----------------------------------------------------------

bool operator==(const data& lhs, const variant_data& rhs);

bool operator==(const variant_data& lhs, const data& rhs);

bool operator==(const variant_data& lhs, const variant_data& rhs);

bool operator<(const variant_data& lhs, const variant_data& rhs);

} // namespace broker
