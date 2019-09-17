#include <benchmark/benchmark.h>

#include <random>
#include <vector>

#include "caf/actor_system.hpp"
#include "caf/actor_system_config.hpp"
#include "caf/binary_serializer.hpp"
#include "caf/scheduler/test_coordinator.hpp"

#include "broker/configuration.hh"
#include "broker/data.hh"

class noexcept_serializer {
public:
  // -- member types -----------------------------------------------------------

  using result_type = caf::error;

  using container_type = std::vector<char>;

  using value_type = typename container_type::value_type;

  // -- constants --------------------------------------------------------------

  static constexpr bool writes_state = false;

  static constexpr bool reads_state = true;

  // -- invariants -------------------------------------------------------------

  static_assert(sizeof(value_type) == 1,
                "container must have a byte-sized value type");

  // -- constructors, destructors, and assignment operators --------------------

  noexcept_serializer(caf::actor_system& sys, container_type& buf)
    : ctx_(sys.dummy_execution_unit()), buf_(buf), write_pos_(buf_.size()) {
    // nop
  }

  noexcept_serializer(caf::execution_unit* ctx, container_type& buf)
    : ctx_(ctx), buf_(buf), write_pos_(buf_.size()) {
    // nop
  }

  // -- position management ----------------------------------------------------

  /// Sets the write position to given offset.
  /// @pre `offset <= buf.size()`
  void seek(size_t offset) {
    write_pos_ = offset;
  }

  /// Jumps `num_bytes` forward. Resizes the buffer (filling it with zeros)
  /// when skipping past the end.
  void skip(size_t num_bytes) {
    auto remaining = buf_.size() - write_pos_;
    if (remaining < num_bytes)
      buf_.insert(buf_.end(), num_bytes - remaining, 0);
    write_pos_ += num_bytes;
  }

  // -- overridden member functions --------------------------------------------

  caf::error begin_object(uint16_t& nr, std::string& name) {
    if (auto err = apply(nr))
      return err;
    if (nr == 0)
      if (auto err = apply(name))
        return err;
    return caf::none;
  }

  caf::error end_object() {
    return caf::none;
  }

  caf::error begin_sequence(size_t& list_size) {
    uint8_t buf[16];
    auto i = buf;
    auto x = static_cast<uint32_t>(list_size);
    while (x > 0x7f) {
      *i++ = (static_cast<uint8_t>(x) & 0x7f) | 0x80;
      x >>= 7;
    }
    *i++ = static_cast<uint8_t>(x) & 0x7f;
    return apply_raw(static_cast<size_t>(i - buf), buf);
  }

  caf::error end_sequence() {
    return caf::none;
  }

  caf::error apply_raw(size_t num_bytes, void* data) {
    CAF_ASSERT(write_pos_ <= buf_.size());
    static_assert((sizeof(value_type) == 1), "sizeof(value_type) > 1");
    auto ptr = reinterpret_cast<value_type*>(data);
    auto buf_size = buf_.size();
    if (write_pos_ == buf_size) {
      buf_.insert(buf_.end(), ptr, ptr + num_bytes);
    } else if (write_pos_ + num_bytes <= buf_size) {
      memcpy(buf_.data() + write_pos_, ptr, num_bytes);
    } else {
      auto remaining = buf_size - write_pos_;
      CAF_ASSERT(remaining < num_bytes);
      memcpy(buf_.data() + write_pos_, ptr, remaining);
      buf_.insert(buf_.end(), ptr + remaining, ptr + num_bytes);
    }
    write_pos_ += num_bytes;
    CAF_ASSERT(write_pos_ <= buf_.size());
    return caf::none;
  }

  template <class T>
  typename std::enable_if<std::is_integral<T>::value
                            && !std::is_same<bool, T>::value,
                          caf::error>::type
  apply(T& x) {
    using type
      = caf::detail::select_integer_type_t<sizeof(T), std::is_signed<T>::value>;
    return apply_impl(reinterpret_cast<type&>(x));
  }

  /// Serializes enums using the underlying type
  /// if no user-defined serialization is defined.
  template <class T>
  typename std::enable_if<std::is_enum<T>::value
                            && !caf::detail::has_serialize<T>::value,
                          caf::error>::type
  apply(T& x) {
    using underlying = typename std::underlying_type<T>::type;
    return apply(reinterpret_cast<underlying&>(x));
  }

  /// Applies this processor to an empty type.
  template <class T>
  typename std::enable_if<std::is_empty<T>::value, caf::error>::type apply(T&) {
    return caf::none;
  }

  caf::error apply(bool& x) {
    uint8_t tmp = x ? 1 : 0;
    return apply(tmp);
  }

  template <class T>
  caf::error consume_range(T& xs) {
    for (auto& x : xs) {
      using value_type = typename std::remove_reference<decltype(x)>::type;
      using mutable_type = typename std::remove_const<value_type>::type;
      if (auto err = apply(const_cast<mutable_type&>(x)))
        return err;
    }
    return caf::none;
  }

  /// Converts each element in `xs` to `U` before calling `apply`.
  template <class U, class T>
  caf::error consume_range_c(T& xs) {
    for (U x : xs)
      if (auto err = apply(x))
        return err;
    return caf::none;
  }

  // Applies this processor as Derived to `xs` in saving mode.
  template <class T>
  typename std::enable_if<!caf::detail::is_byte_sequence<T>::value,
                          caf::error>::type
  apply_sequence(T& xs) {
    auto s = xs.size();
    if (auto err = begin_sequence(s))
      return err;
    if (auto err = consume_range(xs))
      return err;
    return end_sequence();
  }

  // Optimized saving for contiguous byte sequences.
  template <class T>
  typename std::enable_if<caf::detail::is_byte_sequence<T>::value,
                          caf::error>::type
  apply_sequence(T& xs) {
    auto s = xs.size();
    if (auto err = begin_sequence(s))
      return err;
    if (s > 0)
      if (auto err = apply_raw(xs.size(), xs.data()))
        return err;
    return end_sequence();
  }

  /// Applies this processor to a sequence of values.
  template <class T>
  typename std::enable_if<
    caf::detail::is_iterable<T>::value && !caf::detail::has_serialize<T>::value
      && !caf::detail::is_inspectable<noexcept_serializer, T>::value,
    caf::error>::type
  apply(T& xs) {
    return apply_sequence(xs);
  }

  template <class F, class S>
  caf::error apply(std::pair<F, S>& xs) {
    using first_type = typename std::remove_const<F>::type;
    using second_type = typename std::remove_const<F>::type;
    if (auto err = apply(const_cast<first_type&>(xs.first)))
      return err;
    return apply(const_cast<second_type&>(xs.second));
  }

  template <class... Ts>
  caf::error apply(std::tuple<Ts...>& xs) {
    return caf::detail::apply_args(*this, caf::detail::get_indices(xs), xs);
  }

  template <class Rep, class Period>
  caf::error apply(std::chrono::duration<Rep, Period>& x) {
    auto count = x.count();
    return apply(count);
  }

  template <class Duration>
  caf::error
  apply(std::chrono::time_point<std::chrono::system_clock, Duration>& t) {
    auto d = t.time_since_epoch();
    return apply(d);
  }

  template <class T>
  typename std::enable_if<
    caf::detail::is_inspectable<noexcept_serializer, T>::value
      && !caf::detail::has_serialize<T>::value && !std::is_empty<T>::value,
    decltype(inspect(std::declval<noexcept_serializer&>(),
                     std::declval<T&>()))>::type
  apply(T& x) {
    return inspect(*this, x);
  }

  // -- properties -------------------------------------------------------------

  container_type& buf() {
    return buf_;
  }

  const container_type& buf() const {
    return buf_;
  }

  size_t write_pos() const noexcept {
    return write_pos_;
  }

  caf::error apply(int8_t& x) {
    return apply_raw(sizeof(int8_t), &x);
  }

  caf::error apply(uint8_t& x) {
    return apply_raw(sizeof(uint8_t), &x);
  }

  caf::error apply(int16_t& x) {
    return apply_int(static_cast<uint16_t>(x));
  }

  caf::error apply(uint16_t& x) {
    return apply_int(x);
  }

  caf::error apply(int32_t& x) {
    return apply_int(static_cast<uint32_t>(x));
  }

  caf::error apply(uint32_t& x) {
    return apply_int(x);
  }

  caf::error apply(int64_t& x) {
    return apply_int(static_cast<uint64_t>(x));
  }

  caf::error apply(uint64_t& x) {
    return apply_int(x);
  }

  caf::error apply(float& x) {
    return apply_int(caf::detail::pack754(x));
  }

  caf::error apply(double& x) {
    return apply_int(caf::detail::pack754(x));
  }

  caf::error apply(long double& x) {
    // The IEEE-754 conversion does not work for long double
    // => fall back to string serialization (event though it sucks).
    std::ostringstream oss;
    oss << std::setprecision(std::numeric_limits<long double>::digits) << x;
    auto tmp = oss.str();
    return apply(tmp);
  }

  caf::error apply(std::string& x) {
    auto str_size = x.size();
    if (auto err = begin_sequence(str_size))
      return err;
    if (str_size > 0) {
      auto data = const_cast<char*>(x.c_str());
      if (auto err = apply_raw(str_size, data))
        return err;
    }
    return end_sequence();
  }

  caf::error apply(std::u16string& x) {
    auto str_size = x.size();
    return begin_sequence(str_size);
    if (str_size > 0) {
      // The standard does not guarantee that char16_t is exactly 16 bits.
      for (auto c : x)
        if (auto err = apply_int(static_cast<uint16_t>(c)))
          return err;
    }
    return end_sequence();
  }

  caf::error apply(std::u32string& x) {
    auto str_size = x.size();
    if (auto err = begin_sequence(str_size))
      return err;
    if (str_size > 0) {
      // The standard does not guarantee that char32_t is exactly 32 bits.
      for (auto c : x)
        if (auto err = apply_int(static_cast<uint32_t>(c)))
          return err;
    }
    return end_sequence();
  }

  caf::error operator()() {
    // End of recursion.
    return caf::none;
  }

  template <class T, class... Ts>
  caf::error operator()(const T& x, const Ts&... xs) {
    if (auto err = apply(const_cast<T&>(x)))
      return err;
    return (*this)(xs...);
  }

private:
  template <class T>
  caf::error apply_int(T x) {
    auto y = caf::detail::to_network_order(x);
    return apply_raw(sizeof(T), &y);
  }

  caf::execution_unit* ctx_;
  container_type& buf_;
  size_t write_pos_;
};

struct lw_error {
  constexpr lw_error(caf::none_t) : code(caf::sec::none) {
    // nop
  }

  constexpr lw_error(caf::sec code = caf::sec::none) : code(code) {
    // nop
  }

  lw_error(const lw_error&) = default;

  lw_error& operator=(const lw_error&) = default;

  explicit operator bool() const noexcept {
    return code != caf::sec::none;
  }

  operator caf::error() const {
    return caf::make_error(code);
  }

  caf::sec code;
};

class noexcept_sec_serializer {
public:
  // -- member types -----------------------------------------------------------

  using result_type = lw_error;

  using container_type = std::vector<char>;

  using value_type = typename container_type::value_type;

  // -- constants --------------------------------------------------------------

  static constexpr bool writes_state = false;

  static constexpr bool reads_state = true;

  // -- invariants -------------------------------------------------------------

  static_assert(sizeof(value_type) == 1,
                "container must have a byte-sized value type");

  // -- constructors, destructors, and assignment operators --------------------

  noexcept_sec_serializer(caf::actor_system& sys, container_type& buf)
    : ctx_(sys.dummy_execution_unit()), buf_(buf), write_pos_(buf_.size()) {
    // nop
  }

  noexcept_sec_serializer(caf::execution_unit* ctx, container_type& buf)
    : ctx_(ctx), buf_(buf), write_pos_(buf_.size()) {
    // nop
  }

  // -- position management ----------------------------------------------------

  /// Sets the write position to given offset.
  /// @pre `offset <= buf.size()`
  void seek(size_t offset) {
    write_pos_ = offset;
  }

  /// Jumps `num_bytes` forward. Resizes the buffer (filling it with zeros)
  /// when skipping past the end.
  void skip(size_t num_bytes) {
    auto remaining = buf_.size() - write_pos_;
    if (remaining < num_bytes)
      buf_.insert(buf_.end(), num_bytes - remaining, 0);
    write_pos_ += num_bytes;
  }

  // -- overridden member functions --------------------------------------------

  lw_error begin_object(uint16_t& nr, std::string& name) {
    if (auto err = apply(nr))
      return err;
    if (nr == 0)
      if (auto err = apply(name))
        return err;
    return caf::none;
  }

  lw_error end_object() {
    return caf::none;
  }

  lw_error begin_sequence(size_t& list_size) {
    uint8_t buf[16];
    auto i = buf;
    auto x = static_cast<uint32_t>(list_size);
    while (x > 0x7f) {
      *i++ = (static_cast<uint8_t>(x) & 0x7f) | 0x80;
      x >>= 7;
    }
    *i++ = static_cast<uint8_t>(x) & 0x7f;
    return apply_raw(static_cast<size_t>(i - buf), buf);
  }

  lw_error end_sequence() {
    return caf::none;
  }

  lw_error apply_raw(size_t num_bytes, void* data) {
    CAF_ASSERT(write_pos_ <= buf_.size());
    static_assert((sizeof(value_type) == 1), "sizeof(value_type) > 1");
    auto ptr = reinterpret_cast<value_type*>(data);
    auto buf_size = buf_.size();
    if (write_pos_ == buf_size) {
      buf_.insert(buf_.end(), ptr, ptr + num_bytes);
    } else if (write_pos_ + num_bytes <= buf_size) {
      memcpy(buf_.data() + write_pos_, ptr, num_bytes);
    } else {
      auto remaining = buf_size - write_pos_;
      CAF_ASSERT(remaining < num_bytes);
      memcpy(buf_.data() + write_pos_, ptr, remaining);
      buf_.insert(buf_.end(), ptr + remaining, ptr + num_bytes);
    }
    write_pos_ += num_bytes;
    CAF_ASSERT(write_pos_ <= buf_.size());
    return caf::none;
  }

  template <class T>
  typename std::enable_if<
    std::is_integral<T>::value && !std::is_same<bool, T>::value, lw_error>::type
  apply(T& x) {
    using type
      = caf::detail::select_integer_type_t<sizeof(T), std::is_signed<T>::value>;
    return apply_impl(reinterpret_cast<type&>(x));
  }

  /// Serializes enums using the underlying type
  /// if no user-defined serialization is defined.
  template <class T>
  typename std::enable_if<std::is_enum<T>::value
                            && !caf::detail::has_serialize<T>::value,
                          lw_error>::type
  apply(T& x) {
    using underlying = typename std::underlying_type<T>::type;
    return apply(reinterpret_cast<underlying&>(x));
  }

  /// Applies this processor to an empty type.
  template <class T>
  typename std::enable_if<std::is_empty<T>::value, lw_error>::type apply(T&) {
    return caf::none;
  }

  lw_error apply(bool& x) {
    uint8_t tmp = x ? 1 : 0;
    return apply(tmp);
  }

  template <class T>
  lw_error consume_range(T& xs) {
    for (auto& x : xs) {
      using value_type = typename std::remove_reference<decltype(x)>::type;
      using mutable_type = typename std::remove_const<value_type>::type;
      if (auto err = apply(const_cast<mutable_type&>(x)))
        return err;
    }
    return caf::none;
  }

  /// Converts each element in `xs` to `U` before calling `apply`.
  template <class U, class T>
  lw_error consume_range_c(T& xs) {
    for (U x : xs)
      if (auto err = apply(x))
        return err;
    return caf::none;
  }

  // Applies this processor as Derived to `xs` in saving mode.
  template <class T>
  typename std::enable_if<!caf::detail::is_byte_sequence<T>::value,
                          lw_error>::type
  apply_sequence(T& xs) {
    auto s = xs.size();
    if (auto err = begin_sequence(s))
      return err;
    if (auto err = consume_range(xs))
      return err;
    return end_sequence();
  }

  // Optimized saving for contiguous byte sequences.
  template <class T>
  typename std::enable_if<caf::detail::is_byte_sequence<T>::value,
                          lw_error>::type
  apply_sequence(T& xs) {
    auto s = xs.size();
    if (auto err = begin_sequence(s))
      return err;
    if (s > 0)
      if (auto err = apply_raw(xs.size(), xs.data()))
        return err;
    return end_sequence();
  }

  /// Applies this processor to a sequence of values.
  template <class T>
  typename std::enable_if<
    caf::detail::is_iterable<T>::value && !caf::detail::has_serialize<T>::value
      && !caf::detail::is_inspectable<noexcept_sec_serializer, T>::value,
    lw_error>::type
  apply(T& xs) {
    return apply_sequence(xs);
  }

  template <class F, class S>
  lw_error apply(std::pair<F, S>& xs) {
    using first_type = typename std::remove_const<F>::type;
    using second_type = typename std::remove_const<F>::type;
    if (auto err = apply(const_cast<first_type&>(xs.first)))
      return err;
    return apply(const_cast<second_type&>(xs.second));
  }

  template <class... Ts>
  lw_error apply(std::tuple<Ts...>& xs) {
    return caf::detail::apply_args(*this, caf::detail::get_indices(xs), xs);
  }

  template <class Rep, class Period>
  lw_error apply(std::chrono::duration<Rep, Period>& x) {
    auto count = x.count();
    return apply(count);
  }

  template <class Duration>
  lw_error
  apply(std::chrono::time_point<std::chrono::system_clock, Duration>& t) {
    auto d = t.time_since_epoch();
    return apply(d);
  }

  template <class T>
  typename std::enable_if<
    caf::detail::is_inspectable<noexcept_sec_serializer, T>::value
      && !caf::detail::has_serialize<T>::value && !std::is_empty<T>::value,
    decltype(inspect(std::declval<noexcept_sec_serializer&>(),
                     std::declval<T&>()))>::type
  apply(T& x) {
    return inspect(*this, x);
  }

  // -- properties -------------------------------------------------------------

  container_type& buf() {
    return buf_;
  }

  const container_type& buf() const {
    return buf_;
  }

  size_t write_pos() const noexcept {
    return write_pos_;
  }

  lw_error apply(int8_t& x) {
    return apply_raw(sizeof(int8_t), &x);
  }

  lw_error apply(uint8_t& x) {
    return apply_raw(sizeof(uint8_t), &x);
  }

  lw_error apply(int16_t& x) {
    return apply_int(static_cast<uint16_t>(x));
  }

  lw_error apply(uint16_t& x) {
    return apply_int(x);
  }

  lw_error apply(int32_t& x) {
    return apply_int(static_cast<uint32_t>(x));
  }

  lw_error apply(uint32_t& x) {
    return apply_int(x);
  }

  lw_error apply(int64_t& x) {
    return apply_int(static_cast<uint64_t>(x));
  }

  lw_error apply(uint64_t& x) {
    return apply_int(x);
  }

  lw_error apply(float& x) {
    return apply_int(caf::detail::pack754(x));
  }

  lw_error apply(double& x) {
    return apply_int(caf::detail::pack754(x));
  }

  lw_error apply(long double& x) {
    // The IEEE-754 conversion does not work for long double
    // => fall back to string serialization (event though it sucks).
    std::ostringstream oss;
    oss << std::setprecision(std::numeric_limits<long double>::digits) << x;
    auto tmp = oss.str();
    return apply(tmp);
  }

  lw_error apply(std::string& x) {
    auto str_size = x.size();
    if (auto err = begin_sequence(str_size))
      return err;
    if (str_size > 0) {
      auto data = const_cast<char*>(x.c_str());
      if (auto err = apply_raw(str_size, data))
        return err;
    }
    return end_sequence();
  }

  lw_error apply(std::u16string& x) {
    auto str_size = x.size();
    return begin_sequence(str_size);
    if (str_size > 0) {
      // The standard does not guarantee that char16_t is exactly 16 bits.
      for (auto c : x)
        if (auto err = apply_int(static_cast<uint16_t>(c)))
          return err;
    }
    return end_sequence();
  }

  lw_error apply(std::u32string& x) {
    auto str_size = x.size();
    if (auto err = begin_sequence(str_size))
      return err;
    if (str_size > 0) {
      // The standard does not guarantee that char32_t is exactly 32 bits.
      for (auto c : x)
        if (auto err = apply_int(static_cast<uint32_t>(c)))
          return err;
    }
    return end_sequence();
  }

  lw_error operator()() {
    // End of recursion.
    return caf::none;
  }

  template <class T, class... Ts>
  lw_error operator()(const T& x, const Ts&... xs) {
    if (auto err = apply(const_cast<T&>(x)))
      return err;
    return (*this)(xs...);
  }

private:
  template <class T>
  lw_error apply_int(T x) {
    auto y = caf::detail::to_network_order(x);
    return apply_raw(sizeof(T), &y);
  }

  caf::execution_unit* ctx_;
  container_type& buf_;
  size_t write_pos_;
};

class custom_serializer {
public:
  // -- member types -----------------------------------------------------------

  using result_type = void;

  using container_type = std::vector<char>;

  using value_type = typename container_type::value_type;

  // -- constants --------------------------------------------------------------

  static constexpr bool writes_state = false;

  static constexpr bool reads_state = true;

  // -- invariants -------------------------------------------------------------

  static_assert(sizeof(value_type) == 1,
                "container must have a byte-sized value type");

  // -- constructors, destructors, and assignment operators --------------------

  custom_serializer(caf::actor_system& sys, container_type& buf)
    : ctx_(sys.dummy_execution_unit()), buf_(buf), write_pos_(buf_.size()) {
    // nop
  }

  custom_serializer(caf::execution_unit* ctx, container_type& buf)
    : ctx_(ctx), buf_(buf), write_pos_(buf_.size()) {
    // nop
  }

  // -- position management ----------------------------------------------------

  /// Sets the write position to given offset.
  /// @pre `offset <= buf.size()`
  void seek(size_t offset) {
    write_pos_ = offset;
  }

  /// Jumps `num_bytes` forward. Resizes the buffer (filling it with zeros)
  /// when skipping past the end.
  void skip(size_t num_bytes) {
    auto remaining = buf_.size() - write_pos_;
    if (remaining < num_bytes)
      buf_.insert(buf_.end(), num_bytes - remaining, 0);
    write_pos_ += num_bytes;
  }

  // -- overridden member functions --------------------------------------------

  void begin_object(uint16_t& nr, std::string& name) {
    apply(nr);
    if (nr == 0)
      apply(name);
  }

  void end_object() {
    // nop
  }

  void begin_sequence(size_t& list_size) {
    // Use varbyte encoding to compress sequence size on the wire.
    // For 64-bit values, the encoded representation cannot get larger than 10
    // bytes. A scratch space of 16 bytes suffices as upper bound.
    uint8_t buf[16];
    auto i = buf;
    auto x = static_cast<uint32_t>(list_size);
    while (x > 0x7f) {
      *i++ = (static_cast<uint8_t>(x) & 0x7f) | 0x80;
      x >>= 7;
    }
    *i++ = static_cast<uint8_t>(x) & 0x7f;
    apply_raw(static_cast<size_t>(i - buf), buf);
  }

  void end_sequence() {
    // nop
  }

  void apply_raw(size_t num_bytes, void* data) {
    CAF_ASSERT(write_pos_ <= buf_.size());
    static_assert((sizeof(value_type) == 1), "sizeof(value_type) > 1");
    auto ptr = reinterpret_cast<value_type*>(data);
    auto buf_size = buf_.size();
    if (write_pos_ == buf_size) {
      buf_.insert(buf_.end(), ptr, ptr + num_bytes);
    } else if (write_pos_ + num_bytes <= buf_size) {
      memcpy(buf_.data() + write_pos_, ptr, num_bytes);
    } else {
      auto remaining = buf_size - write_pos_;
      CAF_ASSERT(remaining < num_bytes);
      memcpy(buf_.data() + write_pos_, ptr, remaining);
      buf_.insert(buf_.end(), ptr + remaining, ptr + num_bytes);
    }
    write_pos_ += num_bytes;
    CAF_ASSERT(write_pos_ <= buf_.size());
  }

  template <class T>
  typename std::enable_if<std::is_integral<T>::value
                          && !std::is_same<bool, T>::value>::type
  apply(T& x) {
    using type
      = caf::detail::select_integer_type_t<sizeof(T), std::is_signed<T>::value>;
    return apply_impl(reinterpret_cast<type&>(x));
  }

  /// Serializes enums using the underlying type
  /// if no user-defined serialization is defined.
  template <class T>
  typename std::enable_if<std::is_enum<T>::value
                          && !caf::detail::has_serialize<T>::value>::type
  apply(T& x) {
    using underlying = typename std::underlying_type<T>::type;
    apply(reinterpret_cast<underlying&>(x));
  }

  /// Applies this processor to an empty type.
  template <class T>
  typename std::enable_if<std::is_empty<T>::value>::type apply(T&) {
    // nop
  }

  void apply(bool& x) {
    uint8_t tmp = x ? 1 : 0;
    apply(tmp);
  }

  template <class T>
  void consume_range(T& xs) {
    for (auto& x : xs) {
      using value_type = typename std::remove_reference<decltype(x)>::type;
      using mutable_type = typename std::remove_const<value_type>::type;
      apply(const_cast<mutable_type&>(x));
    }
  }

  /// Converts each element in `xs` to `U` before calling `apply`.
  template <class U, class T>
  void consume_range_c(T& xs) {
    for (U x : xs)
      apply(x);
  }

  // Applies this processor as Derived to `xs` in saving mode.
  template <class T>
  typename std::enable_if<!caf::detail::is_byte_sequence<T>::value>::type
  apply_sequence(T& xs) {
    auto s = xs.size();
    begin_sequence(s);
    consume_range(xs);
    end_sequence();
  }

  // Optimized saving for contiguous byte sequences.
  template <class T>
  typename std::enable_if<caf::detail::is_byte_sequence<T>::value>::type
  apply_sequence(T& xs) {
    auto s = xs.size();
    begin_sequence(s);
    if (s > 0)
      apply_raw(xs.size(), xs.data());
    end_sequence();
  }

  /// Applies this processor to a sequence of values.
  template <class T>
  typename std::enable_if<
    caf::detail::is_iterable<T>::value && !caf::detail::has_serialize<T>::value
    && !caf::detail::is_inspectable<custom_serializer, T>::value>::type
  apply(T& xs) {
    return apply_sequence(xs);
  }

  template <class F, class S>
  void apply(std::pair<F, S>& xs) {
    apply(const_cast<typename std::remove_const<F>::type&>(xs.first));
    apply(const_cast<typename std::remove_const<S>::type&>(xs.second));
  }

  template <class... Ts>
  void apply(std::tuple<Ts...>& xs) {
    return caf::detail::apply_args(*this, caf::detail::get_indices(xs), xs);
  }

  template <class Rep, class Period>
  void apply(std::chrono::duration<Rep, Period>& x) {
    auto count = x.count();
    apply(count);
  }

  template <class Duration>
  void apply(std::chrono::time_point<std::chrono::system_clock, Duration>& t) {
    auto d = t.time_since_epoch();
    apply(d);
  }

  template <class T>
  typename std::enable_if<
    caf::detail::is_inspectable<custom_serializer, T>::value
      && !caf::detail::has_serialize<T>::value && !std::is_empty<T>::value,
    decltype(inspect(std::declval<custom_serializer&>(),
                     std::declval<T&>()))>::type
  apply(T& x) {
    inspect(*this, x);
  }

  // -- properties -------------------------------------------------------------

  container_type& buf() {
    return buf_;
  }

  const container_type& buf() const {
    return buf_;
  }

  size_t write_pos() const noexcept {
    return write_pos_;
  }

  void apply(int8_t& x) {
    return apply_raw(sizeof(int8_t), &x);
  }

  void apply(uint8_t& x) {
    return apply_raw(sizeof(uint8_t), &x);
  }

  void apply(int16_t& x) {
    return apply_int(static_cast<uint16_t>(x));
  }

  void apply(uint16_t& x) {
    return apply_int(x);
  }

  void apply(int32_t& x) {
    return apply_int(static_cast<uint32_t>(x));
  }

  void apply(uint32_t& x) {
    return apply_int(x);
  }

  void apply(int64_t& x) {
    return apply_int(static_cast<uint64_t>(x));
  }

  void apply(uint64_t& x) {
    return apply_int(x);
  }

  void apply(float& x) {
    return apply_int(caf::detail::pack754(x));
  }

  void apply(double& x) {
    return apply_int(caf::detail::pack754(x));
  }

  void apply(long double& x) {
    // The IEEE-754 conversion does not work for long double
    // => fall back to string serialization (event though it sucks).
    std::ostringstream oss;
    oss << std::setprecision(std::numeric_limits<long double>::digits) << x;
    auto tmp = oss.str();
    return apply(tmp);
  }

  void apply(std::string& x) {
    auto str_size = x.size();
    begin_sequence(str_size);
    if (str_size > 0) {
      auto data = const_cast<char*>(x.c_str());
      apply_raw(str_size, data);
    }
    end_sequence();
  }

  void apply(std::u16string& x) {
    auto str_size = x.size();
    begin_sequence(str_size);
    if (str_size > 0) {
      // The standard does not guarantee that char16_t is exactly 16 bits.
      for (auto c : x)
        apply_int(static_cast<uint16_t>(c));
    }
    end_sequence();
  }

  void apply(std::u32string& x) {
    auto str_size = x.size();
    begin_sequence(str_size);
    if (str_size > 0) {
      // The standard does not guarantee that char32_t is exactly 32 bits.
      for (auto c : x)
        apply_int(static_cast<uint32_t>(c));
    }
    end_sequence();
  }

  void operator()() {
    // End of recursion.
  }

  template <class T, class... Ts>
  void operator()(const T& x, const Ts&... xs) {
    apply(const_cast<T&>(x));
    (*this)(xs...);
  }

private:
  template <class T>
  void apply_int(T x) {
    auto y = caf::detail::to_network_order(x);
    return apply_raw(sizeof(T), &y);
  }

  caf::execution_unit* ctx_;
  container_type& buf_;
  size_t write_pos_;
};

struct config : caf::actor_system_config {
  config() {
    using caf::atom;
    set("scheduler.policy", caf::atom("testing"));
    set("logger.inline-output", true);
    broker::configuration::add_message_types(*this);
  }
};

struct SerializerFixture : benchmark::Fixture {
    using scheduler_type = caf::scheduler::test_coordinator;

  std::vector<char> buf;
  broker::data recursive;
  caf::message wrapped_recursive;
  config cfg;
  caf::actor_system sys;
  scheduler_type& sched;
  SerializerFixture()
    : sys(cfg), sched(dynamic_cast<scheduler_type&>(sys.scheduler())) {
    sched.run();
    buf.reserve(2048);
    broker::table m;
    std::minstd_rand engine{0};
    std::uniform_int_distribution<char> char_generator{'!', '}'};
    auto random_string=[&](size_t n) {
      std::string result;
      for (size_t i = 0; i < n; ++i)
        result += char_generator(engine);
      return result;
    };
    for (int i = 0; i < 100; i++) {
      broker::set s;
      for (int j = 0; j < 10; j++)
        s.insert(random_string(5));
      m[random_string(15)] = s;
    }
    recursive = broker::vector{broker::now(), m};
    wrapped_recursive = caf::make_message(recursive);
    // Sanity check.
    std::vector<char> buf1;
    caf::binary_serializer bs{nullptr, buf1};
    inspect(bs, recursive);
    std::vector<char> buf2;
    custom_serializer cs{nullptr, buf2};
    inspect(cs, recursive);
    if (buf1 != buf2) {
      std::cerr << "buffers differ!" << std::endl;
      throw std::logic_error("custom_serializer produces wrong buffer");
    }
  }

  ~SerializerFixture() {
    sched.run();
  }
};

BENCHMARK_DEFINE_F(SerializerFixture, BinarySerializer)
(benchmark::State& state) {
  for (auto _ : state) {
    buf.clear();
    caf::binary_serializer bs{sys, buf};
    inspect(bs, recursive);
    benchmark::DoNotOptimize(buf);
  }
}

BENCHMARK_REGISTER_F(SerializerFixture, BinarySerializer);

BENCHMARK_DEFINE_F(SerializerFixture, NoexceptSerializer)
(benchmark::State& state) {
  for (auto _ : state) {
    buf.clear();
    noexcept_serializer bs{sys, buf};
    inspect(bs, recursive);
    benchmark::DoNotOptimize(buf);
  }
}

BENCHMARK_REGISTER_F(SerializerFixture, NoexceptSerializer);

BENCHMARK_DEFINE_F(SerializerFixture, NoexceptSecSerializer)
(benchmark::State& state) {
  for (auto _ : state) {
    buf.clear();
    noexcept_sec_serializer bs{sys, buf};
    inspect(bs, recursive);
    benchmark::DoNotOptimize(buf);
  }
}

BENCHMARK_REGISTER_F(SerializerFixture, NoexceptSecSerializer);

BENCHMARK_DEFINE_F(SerializerFixture, CustomSerializer)
(benchmark::State& state) {
  for (auto _ : state) {
    buf.clear();
    custom_serializer bs{sys, buf};
    inspect(bs, recursive);
    benchmark::DoNotOptimize(buf);
  }
}

BENCHMARK_REGISTER_F(SerializerFixture, CustomSerializer);

BENCHMARK_DEFINE_F(SerializerFixture, BinarySerializerWrappedData)
(benchmark::State& state) {
  for (auto _ : state) {
    buf.clear();
    caf::binary_serializer bs{sys, buf};
    inspect(bs, wrapped_recursive);
    benchmark::DoNotOptimize(buf);
  }
}

BENCHMARK_REGISTER_F(SerializerFixture, BinarySerializerWrappedData);

/*
BENCHMARK_DEFINE_F(SerializerFixture, NoexceptSerializerWrapped)
(benchmark::State& state) {
  for (auto _ : state) {
    buf.clear();
    noexcept_serializer bs{nullptr, buf};
    inspect(bs, wrapped_recursive);
    benchmark::DoNotOptimize(buf);
  }
}

BENCHMARK_REGISTER_F(SerializerFixture, NoexceptSerializerWrapped);

BENCHMARK_DEFINE_F(SerializerFixture, CustomSerializerWrapped)
(benchmark::State& state) {
  for (auto _ : state) {
    buf.clear();
    custom_serializer bs{nullptr, buf};
    inspect(bs, wrapped_recursive);
    benchmark::DoNotOptimize(buf);
  }
}

BENCHMARK_REGISTER_F(SerializerFixture, CustomSerializerWrapped);
*/

BENCHMARK_MAIN();
