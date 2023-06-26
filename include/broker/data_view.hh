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
  virtual data_view get_data() const noexcept = 0;

  /// Returns the topic for the data in this envelope.
  virtual const topic& get_topic() const noexcept = 0;

  /// Checks whether `val` is the root value.
  virtual bool is_root(const detail::data_view_value* val) const noexcept = 0;

  /// Returns the raw bytes of the serialized data stored in this envelope or a
  /// range of size 0 if the data is not available in serialized form.
  virtual std::pair<const std::byte*, size_t> raw_bytes() const noexcept = 0;

  /// Creates a new data envolope from the given @ref topic and @ref data.
  static data_envelope_ptr make(topic t, const data& d);

  /// Creates a new data envolope from the given @ref topic and @ref data.
  static data_envelope_ptr make(topic t, data_view d);

protected:
  /// Parses the data returned from @ref raw_bytes.
  detail::data_view_value* do_parse(detail::monotonic_buffer_resource& buf,
                                    error& err);
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

bool operator<(const data_view_value& lhs, const data_view_value& rhs) noexcept;

} // namespace broker::detail

namespace broker {

/// Evaluates to `true` if `T` is a primitive type that can be passed to a
/// `vector_builder` by value.
template <class T>
inline constexpr bool is_primtivie_data_v =
  detail::is_one_of_v<T, none, boolean, count, integer, real, std::string_view,
                      address, subnet, port, timestamp, timespan,
                      enum_value_view>;

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

  /// Checks whether this object is the root object in its envelope.
  bool is_root() const noexcept {
    return envelope_ && envelope_->is_root(value_);
  }

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

  /// @private
  const auto& as_variant() const noexcept {
    return value_->data;
  }

  /// @private
  const auto* raw_ptr() const noexcept {
    return value_;
  }

  /// @private
  const auto* envelope_ptr() const noexcept {
    return envelope_.get();
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

/// Checks whether a data object can be converted to `T`.
template <class T>
bool exact_match_or_can_convert_to(const data_view& x) {
  if constexpr (detail::data_tag_oracle<T>::specialized) {
    return x.get_type() == detail::data_tag_oracle<T>::value;
  } else if constexpr (std::is_same_v<any_type, T>) {
    return true;
  } else {
    return can_convert_to<T>(x);
  }
}

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
      return data_view{std::addressof(*pos_), shared_envelope()};
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

    data_envelope_ptr shared_envelope() const {
      if (envelope_)
        return envelope_->shared_from_this();
      return nullptr;
    }

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

  // -- element access ---------------------------------------------------------

  // TODO: this is only implemented for compatibility with broker::vector API
  //       and to have existing algorithms work with data_view. Should be
  //       removed eventually, because it is very inefficient.
  data_view operator[](size_t index) const noexcept {
    auto i = values_->begin();
    std::advance(i, index);
    return data_view{std::addressof(*i), envelope_};
  }

  data_view front() const noexcept {
    return data_view{std::addressof(values_->front()), envelope_};
  }

  data_view back() const noexcept {
    return data_view{std::addressof(values_->back()), envelope_};
  }

  // -- properties -------------------------------------------------------------

  /// @private
  const auto* raw_ptr() const noexcept {
    return values_;
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

inline bool contains_impl(vector_view::iterator, detail::parameter_pack<>) {
  return true;
}

template <class T, class... Ts>
bool contains_impl(vector_view::iterator pos,
                   detail::parameter_pack<T, Ts...>) {
  if (!exact_match_or_can_convert_to<T>(*pos++))
    return false;
  return contains_impl(pos, detail::parameter_pack<Ts...>{});
}

/// Checks whether `xs` contains values of types `Ts...`. Performs "fuzzy"
/// matching by calling `can_convert_to<T>` for any `T` that is not part of the
/// variant.
template <class... Ts>
bool contains(const vector_view& xs) {
  if (xs.size() != sizeof...(Ts))
    return false;
  return contains_impl(xs.begin(), detail::parameter_pack<Ts...>{});
}

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
      return data_view{std::addressof(*pos_), shared_envelope()};
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

    data_envelope_ptr shared_envelope() const {
      if (envelope_)
        return envelope_->shared_from_this();
      return nullptr;
    }

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

  // -- properties -------------------------------------------------------------

  /// @private
  const auto* raw_ptr() const noexcept {
    return values_;
  }

  /// Checks whether this set contains `what`.
  template <class T>
  std::enable_if_t<is_primtivie_data_v<T>, bool>
  contains(T what) const noexcept {
    detail::data_view_value tmp{what};
    return values_->find(tmp) != values_->end();
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
      return data_view{std::addressof(pos_->first), shared_envelope()};
    }

    data_view value() const noexcept {
      return data_view{std::addressof(pos_->second), shared_envelope()};
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

    data_envelope_ptr shared_envelope() const {
      if (envelope_)
        return envelope_->shared_from_this();
      return nullptr;
    }

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

  /// @private
  const auto* raw_ptr() const noexcept {
    return values_;
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

/// Converts `what` to a string.
void convert(const data_view& what, std::string& out);

/// Converts `what` to a string.
void convert(const set_view& what, std::string& out);

/// Converts `what` to a string.
void convert(const table_view& what, std::string& out);

/// Converts `what` to a string.
void convert(const vector_view& what, std::string& out);

/// Prints `what` to `out`.
std::ostream& operator<<(std::ostream& out, const data_view& what);

/// Prints `what` to `out`.
std::ostream& operator<<(std::ostream& out, const set_view& what);

/// Prints `what` to `out`.
std::ostream& operator<<(std::ostream& out, const table_view& what);

/// Prints `what` to `out`.
std::ostream& operator<<(std::ostream& out, const vector_view& what);

} // namespace broker
