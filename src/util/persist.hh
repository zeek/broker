#ifndef BROKER_UTIL_PERSIST_HH
#define BROKER_UTIL_PERSIST_HH

#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <type_traits>
#include <typeindex>
#include <typeinfo>
#include <unordered_map>

namespace broker {
namespace util {
namespace persist {

/// Declare the current persistence version tag number for a given class/struct.
#define BROKER_PERSIST_VERSION(TYPE, NUMBER)                                   \
  template <>                                                                  \
  const uint32_t broker::util::persist::version<TYPE>::number = NUMBER;

/// A persistence version tag for class/struct type \a T.
template <class T>
struct version {
  static const uint32_t number;
};

using version_map_type = std::unordered_map<std::type_index, uint32_t>;

/// Allows for versioned serialization of a user-defined class/struct.
/// Use BROKER_PERSIST_VERSION to declare the current version of that type and
/// then define free functions to serialize/deserialize instances of it:
///
/// - void save(persist::save_archive&, const T&, uint32_t version);
/// - void load(persist::load_archive&, T&, uint32_t version);
///
/// Pointers are not specifically handled (e.g. to reduce serialized size or
/// handle cycles), that's up to user to work out.
class save_archive {
public:
  save_archive() = default;

  /// Initialize archive with a buffer that contains bytes to serialize as-is.
  save_archive(std::string serial_bytes);

  /// Re-initialize the archive.
  void reset(std::string serial_bytes = "");

  /// @return the serialized bytes and reset the archive.
  std::string get();

  /// Serialize some number of bytes.
  friend save_archive& save_binary(save_archive& ar, const void* bytes,
                                   size_t size);

  /// Serialize a primitive data type.
  template <class T>
  friend
  typename std::enable_if<std::is_arithmetic<T>::value, save_archive&>::type
  save(save_archive& ar, const T& t);

  /// Serialize a user-defined type.
  template <class T>
  friend
  typename std::enable_if<!std::is_arithmetic<T>::value, save_archive&>::type
  save(save_archive& ar, const T& t);

private:
  void save_bytes(const uint8_t* bytes, size_t size);

  void save_bytes_reverse(const uint8_t* bytes, size_t size);

  void save_value(const uint8_t* bytes, size_t size);

  uint32_t register_class(const std::type_info& ti, uint32_t current_version);

  std::string serial_;
  version_map_type version_map_;
};

/// Serialize a size which refers to a number of items in a sequence which
/// immediately follows.
save_archive& save_sequence(save_archive& ar, size_t size);

template <class T>
typename std::enable_if<std::is_arithmetic<T>::value, save_archive&>::type
save(save_archive& ar, const T& t) {
  static_assert(!std::is_floating_point<T>::value
                  || (std::is_floating_point<T>::value
                      && std::numeric_limits<T>::is_iec559),
                "persistence only supports IEEE 754 floating point");
  ar.save_value(reinterpret_cast<const uint8_t*>(&t), sizeof(t));
  return ar;
}

template <class T>
typename std::enable_if<!std::is_arithmetic<T>::value, save_archive&>::type
save(save_archive& ar, const T& t) {
  auto version_number = ar.register_class(typeid(T), version<T>::number);
  save(ar, t, version_number);
  return ar;
}

/// Allows for versioned deserialization of a user-defined class/struct.
/// @see broker::util::persist::save_archive
class load_archive {
public:
  load_archive() = default;

  /// Initialize the archive with some serialized data.  Memory must remain
  /// valid for any subsequent operations on the archive (a copy is not made).
  load_archive(const void* bytes, size_t num_bytes);

  /// Re-initialize the archive with some serialized data.
  void reset(const void* bytes, size_t num_bytes);

  /// Load some number of bytes from the archive
  friend load_archive& load_binary(load_archive& ar, std::string* rval);

  /// Load a primitive type from the archive.
  template <class T>
  friend typename std::enable_if<
    std::is_arithmetic<T>::value, load_archive&
  >::type load(load_archive& ar, T* t);

  /// Load a user-defined type from the archive.
  template <class T>
  friend typename std::enable_if<
    !std::is_arithmetic<T>::value, load_archive&
  >::type load(load_archive& ar, T* t);

private:
  template <class T>
  T load_value() {
    T rval;
    load_value(reinterpret_cast<uint8_t*>(&rval), sizeof(rval));
    return rval;
  }

  void load_value(uint8_t* dst, size_t size);

  uint32_t register_class(const std::type_info& ti);

  const void* serial_bytes_;
  size_t num_bytes_;
  size_t position_ = 0;
  std::unordered_map<std::type_index, uint32_t> version_map_;
};

/// Load a sequence size from the archive.
uint64_t load_sequence(load_archive& ar);

template <class T>
typename std::enable_if<std::is_arithmetic<T>::value, load_archive&>::type
load(load_archive& ar, T* t) {
  static_assert(!std::is_floating_point<T>::value
                  || (std::is_floating_point<T>::value
                      && std::numeric_limits<T>::is_iec559),
                "persistence only supports IEEE 754 floating point");
  *t = ar.load_value<T>();
  return ar;
}

template <class T>
typename std::enable_if<!std::is_arithmetic<T>::value, load_archive&>::type
load(load_archive& ar, T* t) {
  auto version_number = ar.register_class(typeid(T));
  load(ar, *t, version_number);
  return ar;
}

} // namespace persist
} // namespace util
} // namespace broker

#endif // BROKER_UTIL_PERSIST_HH
