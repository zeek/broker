#pragma once

#include <cstddef>
#include <iterator>
#include <optional>

#include "broker/data.hh"
#include "broker/detail/assert.hh"

namespace broker::zeek {

const count ProtocolVersion = 1;

/// Metadata attached to Zeek events.
enum class MetadataType : uint8_t {
  NetworkTimestamp = 1,
  // 1 - 199 is reserved for Zeek. Otherwise free for external
  // users to experiment with before potentially including
  // in the reserved range.
  UserMetadataStart = 200,
};

/// Generic Zeek-level message.
class Message {
public:
  enum Type {
    Invalid = 0,
    Event = 1,
    LogCreate = 2,
    LogWrite = 3,
    IdentifierUpdate = 4,
    Batch = 5,
    MAX = Batch,
  };

  Type type() const {
    if (as_vector().size() < 2)
      return Type::Invalid;

    auto cp = get_if<count>(&as_vector()[1]);

    if (!cp)
      return Type::Invalid;

    if (*cp > Type::MAX)
      return Type::Invalid;

    return Type(*cp);
  }

  data&& move_data() {
    return std::move(data_);
  }

  const data& as_data() const {
    return data_;
  }

  data& as_data() {
    return data_;
  }

  const vector& as_vector() const {
    return get<vector>(data_);
  }

  vector& as_vector() {
    return get<vector>(data_);
  }

  operator data() const {
    return as_data();
  }

  static Type type(const data& msg) {
    auto vp = get_if<vector>(&msg);

    if (!vp)
      return Type::Invalid;

    auto& v = *vp;

    if (v.size() < 2)
      return Type::Invalid;

    auto cp = get_if<count>(&v[1]);

    if (!cp)
      return Type::Invalid;

    if (*cp > Type::MAX)
      return Type::Invalid;

    return Type(*cp);
  }

protected:
  Message(Type type, vector content)
    : data_(vector{ProtocolVersion, count(type), std::move(content)}) {}

  Message(data msg) : data_(std::move(msg)) {}

  data data_;
};

/// Support iteration with structured binding.
class MetadataIterator {
public:
  using iterator_category = std::forward_iterator_tag;
  using difference_type = std::ptrdiff_t;
  using value_type = std::pair<count, const data&>;
  using pointer = value_type*;
  using reference = value_type&;

  explicit MetadataIterator(const broker::data* ptr) noexcept : ptr_(ptr) {}

  MetadataIterator(const MetadataIterator&) noexcept = default;

  MetadataIterator& operator=(const MetadataIterator&) noexcept = default;

  value_type operator*() const {
    auto entry_ptr = get_if<vector>(*ptr_);
    BROKER_ASSERT(entry_ptr && entry_ptr->size() == 2);
    const auto& entry = *entry_ptr;
    return {get<count>(entry[0]), entry[1]};
  }

  MetadataIterator& operator++() noexcept {
    ++ptr_;
    return *this;
  }

  MetadataIterator operator++(int) noexcept {
    return MetadataIterator{ptr_++};
  }

  bool operator!=(const MetadataIterator& other) const noexcept {
    return ptr_ != other.ptr_;
  }

  bool operator==(const MetadataIterator& other) const noexcept {
    return ptr_ == other.ptr_;
  }

private:
  const broker::data* ptr_;
};

/// Supports iteration over metadata
class MetadataWrapper {
public:
  explicit MetadataWrapper(const vector* v) noexcept : v_(v) {}

  [[nodiscard]] MetadataIterator begin() const noexcept {
    return MetadataIterator{v_ ? v_->data() : nullptr};
  }

  [[nodiscard]] MetadataIterator end() const noexcept {
    return MetadataIterator{v_ ? v_->data() + v_->size() : nullptr};
  }

  const data* value(MetadataType key) const {
    return value(static_cast<count>(key));
  }

  const data* value(count key) const {
    for (const auto& [k, v] : *this) {
      if (k == key)
        return &v;
    }
    return nullptr;
  }

  /// Raw access to the underlying metadata vector.
  const vector* get_vector() const noexcept {
    return v_;
  }

private:
  const vector* v_;
};

/// A Zeek event.
class Event : public Message {
public:
  Event(std::string name, vector args)
    : Message(Message::Type::Event, {std::move(name), std::move(args)}) {}

  Event(std::string name, vector args, timestamp ts)
    : Message(Message::Type::Event,
              {std::move(name), std::move(args),
               vector{{vector{
                 static_cast<count>(MetadataType::NetworkTimestamp), ts}}}}) {}

  Event(std::string name, vector args, vector metadata)
    : Message(Message::Type::Event,
              {std::move(name), std::move(args), std::move(metadata)}) {}

  Event(data msg) : Message(std::move(msg)) {}

  const std::string& name() const {
    return get<std::string>(get<vector>(as_vector()[2])[0]);
  }

  std::string& name() {
    return get<std::string>(get<vector>(as_vector()[2])[0]);
  }

  MetadataWrapper metadata() const {
    if (const auto* ev_vec_ptr = get_if<vector>(as_vector()[2]);
        ev_vec_ptr && ev_vec_ptr->size() >= 3)
      return MetadataWrapper{get_if<vector>((*ev_vec_ptr)[2])};

    return MetadataWrapper{nullptr};
  }

  const std::optional<timestamp> ts() const {
    if (auto ts_ptr = metadata().value(MetadataType::NetworkTimestamp))
      return get<timestamp>(*ts_ptr);

    return std::nullopt;
  }

  const vector& args() const {
    return get<vector>(get<vector>(as_vector()[2])[1]);
  }

  vector& args() {
    return get<vector>(get<vector>(as_vector()[2])[1]);
  }

  bool valid() const {
    if (as_vector().size() < 3)
      return false;

    auto vp = get_if<vector>(&(as_vector()[2]));

    if (!vp)
      return false;

    auto& v = *vp;

    if (v.size() < 2)
      return false;

    auto name_ptr = get_if<std::string>(&v[0]);

    if (!name_ptr)
      return false;

    auto args_ptr = get_if<vector>(&v[1]);

    if (!args_ptr)
      return false;

    // Optional event metadata verification.
    //
    // Verify the third element if it exists is a vector<vector<count, data>>
    // and type and further check that the NetworkTimestamp metadata has the
    // right type because we know down here what to expect.
    if (v.size() > 2) {
      auto md_ptr = get_if<vector>(&v[2]);
      if (!md_ptr)
        return false;

      for (const auto& mde : *md_ptr) {
        auto mdev_ptr = get_if<vector>(mde);
        if (!mdev_ptr)
          return false;

        if (mdev_ptr->size() != 2)
          return false;

        const auto& mdev = *mdev_ptr;

        auto mde_key_ptr = get_if<count>(mdev[0]);
        if (!mde_key_ptr)
          return false;

        constexpr auto net_ts_key =
          static_cast<count>(MetadataType::NetworkTimestamp);
        if (*mde_key_ptr == net_ts_key) {
          auto mde_val_ptr = get_if<timestamp>(mdev[1]);
          if (!mde_val_ptr)
            return false;
        }
      }
    }

    return true;
  }
};

/// A batch of other messages.
class Batch : public Message {
public:
  Batch(vector msgs) : Message(Message::Type::Batch, std::move(msgs)) {}

  Batch(data msg) : Message(std::move(msg)) {}

  const vector& batch() const {
    return get<vector>(as_vector()[2]);
  }

  vector& batch() {
    return get<vector>(as_vector()[2]);
  }

  bool valid() const {
    if (as_vector().size() < 3)
      return false;

    auto vp = get_if<vector>(&(as_vector()[2]));

    if (!vp)
      return false;

    return true;
  }
};

/// A Zeek log-create message. Note that at the moment this should be used
/// only by Zeek itself as the arguments aren't pulbically defined.
class LogCreate : public Message {
public:
  LogCreate(enum_value stream_id, enum_value writer_id, data writer_info,
            data fields_data)
    : Message(Message::Type::LogCreate,
              {std::move(stream_id), std::move(writer_id),
               std::move(writer_info), std::move(fields_data)}) {}

  LogCreate(data msg) : Message(std::move(msg)) {}

  const enum_value& stream_id() const {
    return get<enum_value>(get<vector>(as_vector()[2])[0]);
  }

  enum_value& stream_id() {
    return get<enum_value>(get<vector>(as_vector()[2])[0]);
  }

  const enum_value& writer_id() const {
    return get<enum_value>(get<vector>(as_vector()[2])[1]);
  }

  enum_value& writer_id() {
    return get<enum_value>(get<vector>(as_vector()[2])[1]);
  }

  const data& writer_info() const {
    return get<vector>(as_vector()[2])[2];
  }

  data& writer_info() {
    return get<vector>(as_vector()[2])[2];
  }

  const data& fields_data() const {
    return get<vector>(as_vector()[2])[3];
  }

  data& fields_data() {
    return get<vector>(as_vector()[2])[3];
  }

  bool valid() const {
    if (as_vector().size() < 3)
      return false;

    auto vp = get_if<vector>(&(as_vector()[2]));

    if (!vp)
      return false;

    auto& v = *vp;

    if (v.size() < 4)
      return false;

    if (!get_if<enum_value>(&v[0]))
      return false;

    if (!get_if<enum_value>(&v[1]))
      return false;

    return true;
  }
};

/// A Zeek log-write message. Note that at the moment this should be used only
/// by Zeek itself as the arguments aren't publicly defined.
class LogWrite : public Message {
public:
  LogWrite(enum_value stream_id, enum_value writer_id, data path,
           data serial_data)
    : Message(Message::Type::LogWrite,
              {std::move(stream_id), std::move(writer_id), std::move(path),
               std::move(serial_data)}) {}

  LogWrite(data msg) : Message(std::move(msg)) {}

  const enum_value& stream_id() const {
    return get<enum_value>(get<vector>(as_vector()[2])[0]);
  }

  enum_value& stream_id() {
    return get<enum_value>(get<vector>(as_vector()[2])[0]);
  }

  const enum_value& writer_id() const {
    return get<enum_value>(get<vector>(as_vector()[2])[1]);
  }

  enum_value& writer_id() {
    return get<enum_value>(get<vector>(as_vector()[2])[1]);
  }

  const data& path() const {
    return get<vector>(as_vector()[2])[2];
  }

  data& path() {
    return get<vector>(as_vector()[2])[2];
  };

  const data& serial_data() const {
    return get<vector>(as_vector()[2])[3];
  }

  data& serial_data() {
    return get<vector>(as_vector()[2])[3];
  }

  bool valid() const {
    if (as_vector().size() < 3)
      return false;

    auto vp = get_if<vector>(&(as_vector()[2]));

    if (!vp)
      return false;

    auto& v = *vp;

    if (v.size() < 4)
      return false;

    if (!get_if<enum_value>(&v[0]))
      return false;

    if (!get_if<enum_value>(&v[1]))
      return false;

    return true;
  }
};

class IdentifierUpdate : public Message {
public:
  IdentifierUpdate(std::string id_name, data id_value)
    : Message(Message::Type::IdentifierUpdate,
              {std::move(id_name), std::move(id_value)}) {}

  IdentifierUpdate(data msg) : Message(std::move(msg)) {}

  const std::string& id_name() const {
    return get<std::string>(get<vector>(as_vector()[2])[0]);
  }

  std::string& id_name() {
    return get<std::string>(get<vector>(as_vector()[2])[0]);
  }

  const data& id_value() const {
    return get<vector>(as_vector()[2])[1];
  }

  data& id_value() {
    return get<vector>(as_vector()[2])[1];
  }

  bool valid() const {
    if (as_vector().size() < 3)
      return false;

    auto vp = get_if<vector>(&(as_vector()[2]));

    if (!vp)
      return false;

    auto& v = *vp;

    if (v.size() < 2)
      return false;

    if (!get_if<std::string>(&v[0]))
      return false;

    return true;
  }
};

} // namespace broker::zeek
