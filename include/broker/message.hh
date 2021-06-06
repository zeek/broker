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
  syn = 0x10,
  syn_ack = 0x20,
  ack = 0x30,
};

/// Tags a packed message with the type of the serialized data. This enumeration
/// is a subset of @ref alm_message_type.
enum class packed_message_type : uint8_t {
  data = 0x01,
  command = 0x02,
};

/// A Broker-internal message with a payload received from the ALM layer.
using packed_message
  = caf::cow_tuple<packed_message_type, alm::multipath, topic,
                   std::shared_ptr<std::vector<std::byte>>>;

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

/// A Broker-internal message between two endpoints.
using node_message_content = caf::variant<data_message, command_message>;

/// Ordered, reliable communication channel between data stores.
using command_channel = detail::channel<entity_id, command_message>;

/// A message for node-to-node communication with either a user-defined data
/// message or a Broker-internal command messages.
using node_message = caf::cow_tuple< // Fields:
  node_message_content,              // 0: content
  alm::multipath>;                   // 1: path

/// Returns whether `x` contains a ::node_message.
inline bool is_data_message(const node_message_content& x) {
  return caf::holds_alternative<data_message>(x);
}

/// Returns whether `x` contains a ::node_message.
inline bool is_data_message(const node_message& x) {
  return is_data_message(get<0>(x));
}

/// Returns whether `x` contains a ::command_message.
inline bool is_command_message(const node_message_content& x) {
  return caf::holds_alternative<command_message>(x);
}

/// Returns whether `x` contains a ::command_message.
inline bool is_command_message(const node_message& x) {
  return is_command_message(get<0>(x));
}

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
template <class Value>
node_message make_node_message(Value&& value, alm::multipath path) {
  return node_message{std::forward<Value>(value), std::move(path)};
}

/// Retrieves the topic from a ::data_message.
inline const topic& get_topic(const data_message& x) {
  return get<0>(x);
}

/// Retrieves the topic from a ::command_message.
inline const topic& get_topic(const command_message& x) {
  return get<0>(x);
}

/// Retrieves the topic from a ::generic_message.
inline const topic& get_topic(const node_message_content& x) {
  if (is_data_message(x))
    return get_topic(caf::get<data_message>(x));
  return get_topic(caf::get<command_message>(x));
}

/// Retrieves the topic from a ::generic_message.
inline const topic& get_topic(const node_message& x) {
  return get_topic(get<0>(x));
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

/// Moves the topic out of a ::node_message. Causes `x` to make a lazy copy of
/// its content if other ::node_message objects hold references to it.
inline topic&& move_topic(node_message_content& x) {
  if (is_data_message(x))
    return move_topic(caf::get<data_message>(x));
  else
    return move_topic(caf::get<command_message>(x));
}

/// Moves the topic out of a ::node_message. Causes `x` to make a lazy copy of
/// its content if other ::node_message objects hold references to it.
inline topic&& move_topic(node_message& x) {
  return move_topic(get<0>(x.unshared()));
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

/// Retrieves the content from a ::data_message.
inline const node_message_content& get_content(const node_message& x) {
  return get<0>(x);
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

/// @copydoc force_unshared
inline void force_unshared(node_message_content& x) {
  if (caf::holds_alternative<data_message>(x))
    force_unshared(get<data_message>(x));
  else
    force_unshared(get<command_message>(x));
}

/// @copydoc force_unshared
inline void force_unshared(node_message& x) {
  x.unshared();
}

/// Moves the content out of a ::node_message. Causes `x` to make a lazy copy of
/// its content if other ::node_message objects hold references to it.
inline node_message_content&& move_content(node_message& x) {
  return std::move(get<0>(x.unshared()));
}

/// Retrieves the path from a ::data_message.
inline const auto& get_path(const node_message& x) {
  return get<1>(x);
}

/// Get unshared access the path field of a ::node_message. Causes `x` to make a
/// lazy copy of its content if other ::node_message objects hold references to
/// it.
inline auto& get_unshared_path(node_message& x) {
  return get<1>(x.unshared());
}

/// Shortcut for `get<data_message>(get_content(x))`.
/// @pre `is_data_message(x)`
inline const data_message& get_data_message(const node_message& x) {
  return get<data_message>(get_content(x));
}

/// Shortcut for `get<command_message>(get_content(x))`.
/// @pre `is_data_message(x)`
inline const command_message& get_command_message(const node_message& x) {
  return get<command_message>(get_content(x));
}

/// Shortcut for `get<data_message>(x)`.
/// @pre `is_data_message(x)`
inline const data_message& get_data_message(const node_message_content& x) {
  return get<data_message>(x);
}

/// Shortcut for `get<data_message>(x)`.
/// @pre `is_data_message(x)`
inline data_message& get_data_message(node_message_content& x) {
  return get<data_message>(x);
}

/// Shortcut for `get<command_message>(x)`.
/// @pre `is_data_message(x)`
inline const command_message& get_command_message(const node_message_content& x) {
  return caf::get<command_message>(x);
}

/// Shortcut for `get<command_message>(x)`.
/// @pre `is_command_message(x)`
inline command_message& get_command_message(node_message_content& x) {
  return caf::get<command_message>(x);
}

/// Shortcut for `get_data(get<command_message>(get_content(x)))`.
/// @pre `is_data_message(x)`
inline const data& get_data(const node_message& x) {
  return get_data(get_data_message(x));
}

/// Converts `msg` to a human-readable string representation.
std::string to_string(const data_message& msg);

/// Converts `msg` to a human-readable string representation.
std::string to_string(const command_message& msg);

/// Converts `msg` to a human-readable string representation.
std::string to_string(const node_message& msg);

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

namespace caf {

template <>
struct inspector_access<std::vector<broker::node_message>>
: inspector_access_base<std::vector<broker::node_message>> {
  using value_type = std::vector<broker::node_message>;

  template <class Inspector>
  static bool load(Inspector& f, value_type& x) {
    auto& paths = broker::detail::thread_local_path_cache();
    auto& topics = broker::detail::thread_local_topic_cache();
    auto& contents = broker::detail::thread_local_content_buf();
    auto ok = f.begin_tuple(3)     //
              && f.apply(paths)    //
              && f.apply(topics)   //
              && f.apply(contents) //
              && f.end_tuple();
    if (ok) {
      x.clear();
      for (auto& [path_index, topic_index, val] : contents) {
        auto* path_ptr = paths.find(path_index);
        auto* topic_ptr = topics.find(topic_index);
        if (path_ptr && topic_ptr) {
          if (holds_alternative<broker::data>(val)) {
            auto& dval = get<broker::data>(val);
            auto dmsg = make_data_message(*topic_ptr, std::move(dval));
            x.emplace_back(make_node_message(std::move(dmsg), *path_ptr));
          } else {
            auto& cval = get<broker::internal_command>(val);
            auto cmsg = make_command_message(*topic_ptr, std::move(cval));
            x.emplace_back(make_node_message(std::move(cmsg), *path_ptr));
          }
        } else {
          f.emplace_error(caf::sec::load_callback_failed,
                          "batch re-assembly failed");
          return false;
        }
      }
      return true;
    } else {
      return false;
    }
  }

  template <class Inspector>
  static bool save(Inspector& f, value_type& x) {
    auto& paths = broker::detail::thread_local_path_cache();
    auto& topics = broker::detail::thread_local_topic_cache();
    auto& contents = broker::detail::thread_local_content_buf();
    paths.clear();
    topics.clear();
    contents.clear();
    for (auto& entry : x) {
      auto& [content, path] = entry.data();
      auto path_index = paths[get_path(entry)];
      if (is_data_message(content)) {
        auto& [topic, value] = get_data_message(content).data();
        auto topic_index = topics[topic];
        contents.emplace_back(topic_index, path_index, value);
      } else {
        auto& [topic, cmd] = get_command_message(content).data();
        auto topic_index = topics[topic];
        contents.emplace_back(topic_index, path_index, cmd);
      }
    }
    return f.begin_tuple(3)     //
           && f.apply(paths)    //
           && f.apply(topics)   //
           && f.apply(contents) //
           && f.end_tuple();
  }

  template <class Inspector>
  static bool apply(Inspector& f, value_type& x) {
    if (!f.has_human_readable_format()) {
      if constexpr (Inspector::is_loading)
        return load(f, x);
      else
        return save(f, x);
    } else {
      return f.list(x);
    }
  }
};

} // namespace caf
