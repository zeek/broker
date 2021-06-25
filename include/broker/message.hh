#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>

#include <caf/async/publisher.hpp>
#include <caf/cow_tuple.hpp>
#include <caf/variant.hpp>

#include "broker/alm/multipath.hh"
#include "broker/data.hh"
#include "broker/internal_command.hh"
#include "broker/topic.hh"

namespace broker {

/// Tags a packed message with the type of the serialized data.
enum class alm_message_type : uint8_t {
  data = 0x01,
  command = 0x02,
  originator_hello = 0x10,
  responder_hello = 0x20,
};

/// Tags a packed message with the type of the serialized data. This enumeration
/// is a subset of @ref alm_message_type.
enum class packed_message_type : uint8_t {
  data = 0x01,
  command = 0x02,
};

/// A Broker-internal message with a payload received from the ALM layer.
using packed_message = caf::cow_tuple<packed_message_type, topic,
                                      std::vector<std::byte>>;

inline auto get_type(const packed_message& msg) {
  return get<0>(msg);
}

inline const topic& get_topic(const packed_message& msg) {
  return get<1>(msg);
}

inline const std::vector<std::byte>& get_payload(const packed_message& msg) {
  return get<2>(msg);
}

/// A Broker-internal message with path and content (packed message).
using node_message = caf::cow_tuple<alm::multipath, packed_message>;

inline const alm::multipath& get_path(const node_message& msg) {
  return get<0>(msg);
}

inline auto get_type(const node_message& msg) {
  return get_type(get<1>(msg));
}

inline const topic& get_topic(const node_message& msg) {
  return get_topic(get<1>(msg));
}

inline const std::vector<std::byte>& get_payload(const node_message& msg) {
  return get_payload(get<1>(msg));
}

/// A user-defined message with topic and data.
using data_message = caf::cow_tuple<topic, data>;

/// A Broker-internal message with topic and command.
using command_message = caf::cow_tuple<topic, internal_command>;

template <class T>
struct packed_message_type_oracle;

template <>
struct packed_message_type_oracle<data_message> {
  static constexpr auto value = packed_message_type::data;
};

template <>
struct packed_message_type_oracle<command_message> {
  static constexpr auto value = packed_message_type::command;
};

template <class T>
constexpr auto packed_message_type_v = packed_message_type_oracle<T>::value;

/// Ordered, reliable communication channel between data stores.
using command_channel = detail::channel<entity_id, command_message>;

/// Generates a ::data_message.
template <class Topic, class Data>
data_message make_data_message(Topic&& t, Data&& d) {
  return data_message(std::forward<Topic>(t), std::forward<Data>(d));
}

/// Generates a ::command_message.
template <class Topic, class Command>
command_message make_command_message(Topic&& t, Command&& d) {
  return command_message(std::forward<Topic>(t), std::forward<Command>(d));
}

/// Generates a ::node_message.
inline node_message make_node_message(alm::multipath path, packed_message pm) {
  return node_message{std::move(path), std::move(pm)};
}

/// Retrieves the topic from a ::data_message.
inline const topic& get_topic(const data_message& x) {
  return get<0>(x);
}

/// Retrieves the topic from a ::command_message.
inline const topic& get_topic(const command_message& x) {
  return get<0>(x);
}

/// Moves the topic out of a ::data_message. Causes `x` to make a lazy copy of
/// its content if other ::data_message objects hold references to it.
inline topic&& move_topic(data_message& x) {
  return std::move(get<0>(x.unshared()));
}

/// Moves the topic out of a ::command_message. Causes `x` to make a lazy copy
/// of its content if other ::command_message objects hold references to it.
inline topic&& move_topic(command_message& x) {
  return std::move(get<0>(x.unshared()));
}

/// Retrieves the data from a @ref data_message.
inline const data& get_data(const data_message& x) {
  return get<1>(x);
}

/// Moves the data out of a @ref data_message. Causes `x` to make a lazy copy of
/// its content if other @ref data_message objects hold references to it.
inline data&& move_data(data_message& x) {
  return std::move(get<1>(x.unshared()));
}

/// Retrieves the command content from a ::command_message.
inline const internal_command& get_command(const command_message& x) {
  return get<1>(x);
}

/// Moves the command content out of a ::command_message. Causes `x` to make a
/// lazy copy of its content if other ::command_message objects hold references
/// to it.
inline internal_command&& move_command(command_message& x) {
  return std::move(get<1>(x.unshared()));
}

/// Force `x` to become uniquely referenced. Performs a deep-copy of the content
/// in case they is more than one reference to it. If `x` is the only object
/// referring to the content, this function does nothing.
inline void force_unshared(data_message& x) {
  x.unshared();
}

/// @copydoc force_unshared
inline void force_unshared(command_message& x) {
  x.unshared();
}

/// Converts `msg` to a human-readable string representation.
std::string to_string(const data_message& msg);

/// Converts `msg` to a human-readable string representation.
std::string to_string(const command_message& msg);

} // namespace broker

// CAF ships node messages in batches. However, simply packing node messages
// into a list can result in a lot of redundant data on the wire. Chances are
// the node messages share some topics or source routing information.
//
// In order to pack data more efficiently on the wire, we specialize
// caf::inspector_access for the batch type and then pull out topics and
// multipaths. The actual payload (either broker::data or internal_command) then
// references topic and path by index and we re-assemble everything back to node
// messages during deserialization.
//
// All intermediary buffers are thread-local variables in order to reduce the
// number of heap allocations. These buffers grow to the size of the largest
// batch during runtime and then reach a state where they no longer need to
// allocate any new memory.

namespace broker::detail {

template <class T>
class indexed_cache {
public:
  using value_type = T;

  uint32_t operator[](const T& val) {
    for (size_t index = 0; index < buf_.size(); ++index)
      if (buf_[index] == val)
        return static_cast<uint32_t>(index);
    auto res = static_cast<uint32_t>(buf_.size());
    buf_.emplace_back(val);
    return res;
  }

  const T* find(uint32_t index) {
    if (index < buf_.size())
      return std::addressof(buf_[index]);
    else
      return nullptr;
  }

  void clear() {
    return buf_.clear();
  }

  template <class Inspector>
  friend bool inspect(Inspector& f, indexed_cache& x) {
    return f.apply(x.buf_);
  }

private:
  std::vector<T> buf_;
};

using topic_cache_type = indexed_cache<topic>;

using path_cache_type = indexed_cache<alm::multipath>;

using content_buf_type = std::vector<
  std::tuple<uint32_t, uint32_t, caf::variant<data, internal_command>>>;

topic_cache_type& thread_local_topic_cache();

path_cache_type& thread_local_path_cache();

content_buf_type& thread_local_content_buf();

struct command_message_publisher {
  caf::async::publisher<command_message> hdl;
};

struct data_message_publisher {
  caf::async::publisher<data_message> hdl;
};

struct packed_message_publisher {
  caf::async::publisher<packed_message> hdl;
};

} // namespace broker::detail
