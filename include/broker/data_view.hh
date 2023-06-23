#pragma once

#include "broker/data.hh"
#include "broker/detail/monotonic_buffer_resource.hh"
#include "broker/error.hh"
#include "broker/expected.hh"

#include <list>
#include <memory>

namespace broker {

/// Wraps a @ref data_view_value and the memory it points into.
class data_envelope : public std::enable_shared_from_this<data_envelope> {
public:
  virtual ~data_envelope();

  /// Returns a view to the root value.
  /// @pre `root != nullptr`
  data_view to_data_view() const noexcept;

  /// Returns the raw bytes of the serialized data stored in this envelope.
  virtual std::pair<const std::byte*, size_t> raw_bytes() const noexcept = 0;

  /// Returns the topic for the data in this envelope.
  virtual const topic& get_topic() const noexcept = 0;

protected:
  /// Parses the data returned from @ref raw_bytes.
  error do_parse();

  /// Provides the memory for all of the parsed data.
  detail::monotonic_buffer_resource buf_;

  /// The root of the data object. Points into the buffer resource.
  detail::data_view_value* root_ = nullptr;
};

/// A shared pointer to a storage object.
using data_envelope_ptr = std::shared_ptr<const data_envelope>;

} // namespace broker

namespace broker::detail {


/// A view into a data object.
class data_view_value {
public:
  // -- member types -----------------------------------------------------------

  using less_type = std::less<>;

  using allocator = monotonic_buffer_resource::allocator<data_view_value>;

  using vector_view = std::list<data_view_value, allocator>;

  using vector_view_iterator = typename vector_view::const_iterator;

  using set_view = std::set<data_view_value, less_type, allocator>;

  using set_view_iterator = typename set_view::const_iterator;

  using table_value = std::pair<const data_view_value, data_view_value>;

  using table_allocator = monotonic_buffer_resource::allocator<table_value>;

  using table_view =
    std::map<data_view_value, data_view_value, less_type, table_allocator>;

  using table_view_iterator = typename table_view::const_iterator;

  using data_variant =
    std::variant<none, boolean, count, integer, real, std::string_view, address,
                 subnet, port, timestamp, timespan, enum_value_view, set_view*,
                 table_view*, vector_view*>;

  // -- static factories -------------------------------------------------------

  /// Singleton for empty data.
  static const data_view_value* nil() noexcept;

  // -- properties -------------------------------------------------------------

  /// Returns the type of the contained data.
  data::type get_type() const noexcept {
    return static_cast<data::type>(data.index());
  }

  // -- conversion -------------------------------------------------------------

  data deep_copy() const;

  // -- member variables -------------------------------------------------------

  data_variant data;
};

template <data::type T>
auto& get(data_view_value& x) {
  return std::get<static_cast<size_t>(T)>(x.data);
}

template <data::type T>
const auto& get(const data_view_value& x) {
  return std::get<static_cast<size_t>(T)>(x.data);
}

bool operator==(const data& lhs, const data_view_value& rhs) noexcept;

bool operator==(const data_view_value& lhs, const data& rhs) noexcept;

inline bool operator<(const data_view_value& lhs,
                      const data_view_value& rhs) noexcept {
  if (lhs.data.index() != rhs.data.index())
    return lhs.data.index() < rhs.data.index();
  return std::visit(
    [&rhs](const auto& x) -> bool {
      using T = std::decay_t<decltype(x)>;
      if constexpr (std::is_same_v<T, data_view_value::set_view*>) {
        return *x < *std::get<data_view_value::set_view*>(rhs.data);
      } else if constexpr (std::is_same_v<T, data_view_value::vector_view*>) {
        return *x < *std::get<data_view_value::vector_view*>(rhs.data);
      } else {
        return x < std::get<T>(rhs.data);
      }
    },
    lhs.data);
}

} // namespace broker::detail

namespace broker {

/// A view into a serialized data object.
class data_view {
public:
  // -- friend types -----------------------------------------------------------

  friend class set_view;
  friend class table_view;

  // -- constructors, destructors, and assignment operators --------------------

  data_view() : value_(detail::data_view_value::nil()) {}

  data_view(data_view&&) = default;

  data_view(const data_view&) = default;

  data_view& operator=(data_view&&) = default;

  data_view& operator=(const data_view&) = default;

  data_view(const detail::data_view_value* value,
            data_envelope_ptr envelope) noexcept
    : value_(value), envelope_(std::move(envelope)) {
    // nop
  }

  // -- properties -------------------------------------------------------------

  /// Returns the type of the contained data.
  data::type get_type() const noexcept {
    return static_cast<data::type>(value_->data.index());
  }

  /// Checks whether this view contains the `nil` value.
  bool is_none() const noexcept {
    return get_type() == data::type::none;
  }

  /// Checks whether this view contains a boolean.
  bool is_boolean() const noexcept {
    return get_type() == data::type::boolean;
  }

  /// Checks whether this view contains a count.
  bool is_count() const noexcept {
    return get_type() == data::type::count;
  }

  /// Checks whether this view contains a integer.
  bool is_integer() const noexcept {
    return get_type() == data::type::integer;
  }

  /// Checks whether this view contains a real.
  bool is_real() const noexcept {
    return get_type() == data::type::real;
  }

  /// Checks whether this view contains a count.
  bool is_string() const noexcept {
    return get_type() == data::type::string;
  }

  /// Checks whether this view contains a count.
  bool is_address() const noexcept {
    return get_type() == data::type::address;
  }

  /// Checks whether this view contains a count.
  bool is_subnet() const noexcept {
    return get_type() == data::type::subnet;
  }

  /// Checks whether this view contains a count.
  bool is_port() const noexcept {
    return get_type() == data::type::port;
  }

  /// Checks whether this view contains a count.
  bool is_timestamp() const noexcept {
    return get_type() == data::type::timestamp;
  }

  /// Checks whether this view contains a count.
  bool is_timespan() const noexcept {
    return get_type() == data::type::timespan;
  }

  /// Checks whether this view contains a count.
  bool is_enum_value() const noexcept {
    return get_type() == data::type::enum_value;
  }

  /// Checks whether this view contains a set.
  bool is_set() const noexcept {
    return get_type() == data::type::set;
  }

  /// Checks whether this view contains a table.
  bool is_table() const noexcept {
    return get_type() == data::type::table;
  }

  /// Checks whether this view contains a vector.
  bool is_vector() const noexcept {
    return get_type() == data::type::vector;
  }

  // -- conversions ------------------------------------------------------------

  /// Converts this view into a @c data object.
  data deep_copy() const;

  /// Retrieves the @c boolean value or returns @p fallback if this object does
  /// not contain a @c boolean.
  bool to_boolean(bool fallback = false) const noexcept {
    if (auto* val = std::get_if<boolean>(&value_->data))
      return *val;
    return fallback;
  }

  /// Retrieves the @c count value or returns @p fallback if this object does
  /// not contain a @c count.
  count to_count(count fallback = 0) const noexcept {
    if (auto* val = std::get_if<count>(&value_->data))
      return *val;
    return fallback;
  }

  /// Retrieves the @c integer value or returns @p fallback if this object does
  /// not contain a @c integer.
  integer to_integer(integer fallback = 0) const noexcept {
    if (auto* val = std::get_if<integer>(&value_->data))
      return *val;
    return fallback;
  }

  /// Retrieves the @c real value or returns @p fallback if this object does
  /// not contain a @c real.
  real to_real(real fallback = 0) const noexcept {
    if (auto* val = std::get_if<real>(&value_->data))
      return *val;
    return fallback;
  }

  /// Retrieves the string value or returns @p fallback if this object does
  /// not contain a string.
  std::string_view to_string(std::string_view fallback = {}) const noexcept {
    if (auto* val = std::get_if<std::string_view>(&value_->data))
      return *val;
    return fallback;
  }

  /// Retrieves the @c address value or returns @p fallback if this object does
  /// not contain a @c address.
  address to_address(address fallback = {}) const noexcept {
    if (auto* val = std::get_if<address>(&value_->data))
      return *val;
    return fallback;
  }

  /// Retrieves the @c subnet value or returns @p fallback if this object does
  /// not contain a @c subnet.
  subnet to_subnet(subnet fallback = {}) const noexcept {
    if (auto* val = std::get_if<subnet>(&value_->data))
      return *val;
    return fallback;
  }

  /// Retrieves the @c port value or returns @p fallback if this object does
  /// not contain a @c port.
  port to_port(port fallback = {}) const noexcept {
    if (auto* val = std::get_if<port>(&value_->data))
      return *val;
    return fallback;
  }

  /// Retrieves the @c timestamp value or returns @p fallback if this object
  /// does not contain a @c timestamp.
  timestamp to_timestamp(timestamp fallback = {}) const noexcept {
    if (auto* val = std::get_if<timestamp>(&value_->data))
      return *val;
    return fallback;
  }

  /// Retrieves the @c timespan value or returns @p fallback if this object does
  /// not contain a @c timespan.
  timespan to_timespan(timespan fallback = {}) const noexcept {
    if (auto* val = std::get_if<timespan>(&value_->data))
      return *val;
    return fallback;
  }

  /// Retrieves the enum_value value or returns @p fallback if this object does
  /// not contain a enum_value.
  enum_value_view to_enum_value(enum_value_view fallback = {}) const noexcept {
    if (auto* val = std::get_if<enum_value_view>(&value_->data))
      return *val;
    return fallback;
  }

  /// Returns the contained values as a set_view or an empty set_view if
  /// this object does not contain a set.
  set_view to_set() const noexcept;

  /// Returns the contained values as a table_view or an empty table_view if
  /// this object does not contain a table.
  table_view to_table() const noexcept;

  /// Returns the contained values as a vector_view or an empty vector_view if
  /// this object does not contain a vector.
  vector_view to_vector() const noexcept;

  // -- accessors --------------------------------------------------------------

  /// Returns the contained value as an instance of @p T.
  /// @pre The contained type must have type @p T.
  template <data::type T>
  const auto& as() const {
    if (auto* ptr = std::get_if<static_cast<size_t>(T)>(&value_->data))
      return *ptr;
    throw bad_variant_access{};
  }

  /// Returns the contained value as an instance of @p T.
  /// @pre The contained type must have type @p T.
  template <data::type T>
  auto& as() {
    if (auto* ptr = std::get_if<static_cast<size_t>(T)>(&value_->data))
      return *ptr;
    throw bad_variant_access{};
  }

  // -- operators --------------------------------------------------------------

  /// Returns a pointer to the underlying data.
  const data_view* operator->() const noexcept {
    // Note: this is only implemented for "drill-down" access from iterators.
    return this;
  }

  // -- comparison operators ---------------------------------------------------

  friend bool operator<(const data_view& x, const data_view& y) noexcept {
    return x.value_->data < y.value_->data;
  }

  friend bool operator<=(const data_view& x, const data_view& y) noexcept {
    return x.value_->data <= y.value_->data;
  }

  friend bool operator>(const data_view& x, const data_view& y) noexcept {
    return x.value_->data > y.value_->data;
  }

  friend bool operator>=(const data_view& x, const data_view& y) noexcept {
    return x.value_->data >= y.value_->data;
  }

  friend bool operator==(const data_view& x, const data_view& y) noexcept {
    return x.value_->data == y.value_->data;
  }

  friend bool operator!=(const data_view& x, const data_view& y) noexcept {
    return x.value_->data != y.value_->data;
  }

  friend bool operator==(const data& lhs, const data_view& rhs) noexcept {
    return lhs == *rhs.value_;
  }

  friend bool operator==(const data_view& lhs, const data& rhs) noexcept {
    return *lhs.value_ == rhs;
  }

private:
  /// The value of this object.
  const detail::data_view_value* value_;

  /// The envelope that holds the data.
  data_envelope_ptr envelope_;
};

/// A view into a list of data objects.
class vector_view {
public:
  // -- friend types -----------------------------------------------------------

  friend class data_view;

  // -- member types -----------------------------------------------------------

  class iterator {
  public:
    friend class vector_view;

    iterator() noexcept = default;

    iterator(const iterator&) noexcept = default;

    iterator& operator=(const iterator&) noexcept = default;

    iterator& operator++() noexcept {
      ++pos_;
      return *this;
    }

    iterator operator++(int) noexcept {
      auto tmp = *this;
      ++pos_;
      return tmp;
    }

    data_view value() const noexcept {
      return data_view{std::addressof(*pos_), envelope_->shared_from_this()};
    }

    data_view operator*() const noexcept {
      return value();
    }

    data_view operator->() const noexcept {
      return value();
    }

    friend bool operator==(const iterator& lhs, const iterator& rhs) noexcept {
      return lhs.pos_ == rhs.pos_;
    }

    friend bool operator!=(const iterator& lhs, const iterator& rhs) noexcept {
      return lhs.pos_ != rhs.pos_;
    }

  private:
    using native_iterator = detail::data_view_value::vector_view_iterator;

    iterator(native_iterator pos, const data_envelope* envelope) noexcept
      : pos_(pos), envelope_(envelope) {
      // nop
    }

    native_iterator pos_;
    const data_envelope* envelope_;
  };

  // -- constructors, destructors, and assignment operators --------------------

  vector_view() noexcept = default;

  vector_view(const vector_view&) noexcept = default;

  bool empty() const noexcept {
    return values_ ? values_->empty() : true;
  }

  size_t size() const noexcept {
    return values_ ? values_->size() : 0u;
  }

  iterator begin() const noexcept {
    return iterator{values_ ? values_->begin() : iterator::native_iterator{},
                    envelope_.get()};
  }

  iterator end() const noexcept {
    return iterator{values_ ? values_->end() : iterator::native_iterator{},
                    envelope_.get()};
  }

private:
  vector_view(const detail::data_view_value::vector_view* values,
              data_envelope_ptr envelope_) noexcept
    : values_(values), envelope_(std::move(envelope_)) {
    // nop
  }

  /// The list of values.
  const detail::data_view_value::vector_view* values_ = nullptr;

  /// The envelope that holds the data.
  data_envelope_ptr envelope_;
};

/// A view into a set of data objects.
class set_view {
public:
  // -- friend types -----------------------------------------------------------

  friend class data_view;

  // -- member types -----------------------------------------------------------

  class iterator {
  public:
    friend class set_view;

    iterator() noexcept = default;

    iterator(const iterator&) noexcept = default;

    iterator& operator=(const iterator&) noexcept = default;

    iterator& operator++() noexcept {
      ++pos_;
      return *this;
    }

    iterator operator++(int) noexcept {
      auto tmp = *this;
      ++pos_;
      return tmp;
    }

    data_view value() const noexcept {
      return data_view{std::addressof(*pos_), envelope_->shared_from_this()};
    }

    data_view operator*() const noexcept {
      return value();
    }

    data_view operator->() const noexcept {
      return value();
    }

    friend bool operator==(const iterator& lhs, const iterator& rhs) noexcept {
      return lhs.pos_ == rhs.pos_;
    }

    friend bool operator!=(const iterator& lhs, const iterator& rhs) noexcept {
      return lhs.pos_ != rhs.pos_;
    }

  private:
    using native_iterator = detail::data_view_value::set_view_iterator;

    iterator(native_iterator pos, const data_envelope* envelope) noexcept
      : pos_(pos), envelope_(envelope) {
      // nop
    }

    native_iterator pos_;
    const data_envelope* envelope_;
  };

  // -- constructors, destructors, and assignment operators --------------------

  set_view() noexcept = default;

  set_view(const set_view&) noexcept = default;

  bool empty() const noexcept {
    return values_ ? values_->empty() : true;
  }

  size_t size() const noexcept {
    return values_ ? values_->size() : 0u;
  }

  iterator begin() const noexcept {
    return iterator{values_ ? values_->begin() : iterator::native_iterator{},
                    envelope_.get()};
  }

  iterator end() const noexcept {
    return iterator{values_ ? values_->end() : iterator::native_iterator{},
                    envelope_.get()};
  }

private:
  set_view(const detail::data_view_value::set_view* values,
           data_envelope_ptr envelope_) noexcept
    : values_(values), envelope_(std::move(envelope_)) {
    // nop
  }

  /// The list of values.
  const detail::data_view_value::set_view* values_ = nullptr;

  /// The envelope that holds the data.
  data_envelope_ptr envelope_;
};

/// A view into a map of data objects.
class table_view {
public:
  // -- friend types -----------------------------------------------------------

  friend class data_view;

  // -- member types -----------------------------------------------------------

  struct key_value_pair {
    data_view first;
    data_view second;

    /// Returns a pointer to the underlying data.
    const key_value_pair* operator->() const noexcept {
      // Note: this is only implemented for "drill-down" access from iterators.
      return this;
    }
  };

  class iterator {
  public:
    friend class table_view;

    iterator() noexcept = default;

    iterator(const iterator&) noexcept = default;

    iterator& operator=(const iterator&) noexcept = default;

    iterator& operator++() noexcept {
      ++pos_;
      return *this;
    }

    iterator operator++(int) noexcept {
      auto tmp = *this;
      ++pos_;
      return tmp;
    }

    data_view key() const noexcept {
      return data_view{std::addressof(pos_->first),
                       envelope_->shared_from_this()};
    }

    data_view value() const noexcept {
      return data_view{std::addressof(pos_->second),
                       envelope_->shared_from_this()};
    }

    key_value_pair operator*() const noexcept {
      return {key(), value()};
    }

    key_value_pair operator->() const noexcept {
      return {key(), value()};
    }

    friend bool operator==(const iterator& lhs, const iterator& rhs) noexcept {
      return lhs.pos_ == rhs.pos_;
    }

    friend bool operator!=(const iterator& lhs, const iterator& rhs) noexcept {
      return lhs.pos_ != rhs.pos_;
    }

  private:
    using native_iterator = detail::data_view_value::table_view_iterator;

    iterator(native_iterator pos, const data_envelope* envelope) noexcept
      : pos_(pos), envelope_(envelope) {
      // nop
    }

    native_iterator pos_;
    const data_envelope* envelope_;
  };

  // -- constructors, destructors, and assignment operators --------------------

  table_view() noexcept = default;

  table_view(const table_view&) noexcept = default;

  // -- properties -------------------------------------------------------------

  bool empty() const noexcept {
    return values_ ? values_->empty() : true;
  }

  size_t size() const noexcept {
    return values_ ? values_->size() : 0u;
  }

  // -- iterator access --------------------------------------------------------

  iterator begin() const noexcept {
    return iterator{values_ ? values_->begin() : iterator::native_iterator{},
                    envelope_.get()};
  }

  iterator end() const noexcept {
    return iterator{values_ ? values_->end() : iterator::native_iterator{},
                    envelope_.get()};
  }

  // -- key lookup -------------------------------------------------------------

  iterator find(const data_view& key) const noexcept {
    return iterator{values_->find(*key.value_), envelope_.get()};
  }

  data_view operator[](const data_view& key) const noexcept {
    if (auto i = values_->find(*key.value_); i != values_->end())
      return data_view{std::addressof(i->second), envelope_};
    return data_view{};
  }

  data_view operator[](std::string_view key) const noexcept {
    return do_lookup(key);
  }

private:
  template <class T>
  data_view do_lookup(T key) const noexcept {
    if (values_ == nullptr)
      return data_view{};
    auto key_view = detail::data_view_value{key};
    if (auto i = values_->find(key_view); i != values_->end())
      return data_view{std::addressof(i->second), envelope_};
    return data_view{};
  }

  table_view(const detail::data_view_value::table_view* values,
             data_envelope_ptr envelope_) noexcept
    : values_(values), envelope_(std::move(envelope_)) {
    // nop
  }

  /// The list of values.
  const detail::data_view_value::table_view* values_ = nullptr;

  /// The envelope that holds the data.
  data_envelope_ptr envelope_;
};

} // namespace broker
