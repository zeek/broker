#pragma once

#include <cstddef>
#include <iterator>
#include <optional>

#include "broker/builder.hh"
#include "broker/data.hh"
#include "broker/data_view.hh"
#include "broker/detail/assert.hh"
#include "broker/topic.hh"

namespace broker::zeek {

constexpr count ProtocolVersion = 1;

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
    return type(data_);
  }

  data deep_copy() const noexcept {
    return data_.deep_copy();
  }

  const data_view& content() const noexcept {
    return data_;
  }

  vector_view as_vector() const noexcept {
    return data_.to_vector();
  }

  static Type type(const data_view& msg) {
    auto vec = msg.to_vector();
    if (vec.size() < 2 || !vec.back().is_count())
      return Type::Invalid;
    if (auto val = vec.back().to_count(); val < Type::MAX)
      return static_cast<Type>(val);
    return Type::Invalid;
  }

protected:
  Message(data_view msg) : data_(std::move(msg)) {}

  data_view data_;
};

/// Support iteration with structured binding.
class MetadataIterator {
public:
  using iterator_category = std::forward_iterator_tag;
  using difference_type = std::ptrdiff_t;
  using value_type = std::pair<count, data_view>;
  using pointer = value_type*;
  using reference = value_type&;

  explicit MetadataIterator(vector_view::iterator raw) noexcept : raw_(raw) {}

  MetadataIterator(const MetadataIterator&) noexcept = default;

  MetadataIterator& operator=(const MetadataIterator&) noexcept = default;

  value_type operator*() const {
    auto entry = raw_->to_vector();
    BROKER_ASSERT(entry.size() == 2);
    return {entry.front().to_count(), entry.back()};
  }

  MetadataIterator& operator++() noexcept {
    ++raw_;
    return *this;
  }

  MetadataIterator operator++(int) noexcept {
    return MetadataIterator{raw_++};
  }

  bool operator!=(const MetadataIterator& other) const noexcept {
    return raw_ != other.raw_;
  }

  bool operator==(const MetadataIterator& other) const noexcept {
    return raw_ == other.raw_;
  }

private:
  vector_view::iterator raw_;
};

/// Supports iteration over metadata
class MetadataWrapper {
public:
  explicit MetadataWrapper(vector_view values) noexcept : values_(values) {}

  [[nodiscard]] MetadataIterator begin() const noexcept {
    return MetadataIterator{values_.begin()};
  }

  [[nodiscard]] MetadataIterator end() const noexcept {
    return MetadataIterator{values_.end()};
  }

  data_view value(MetadataType key) const {
    return value(static_cast<count>(key));
  }

  data_view value(count key) const {
    for (const auto& [k, v] : *this) {
      if (k == key)
        return v;
    }
    return {};
  }

  bool empty() const noexcept {
    return values_.empty();
  }

  /// Raw access to the underlying metadata vector.
  vector_view get_vector() const noexcept {
    return values_;
  }

private:
  vector_view values_;
};

/// A builder object for constructing metadata.
class MetadataBuilder {
public:
  template <class T>
  MetadataBuilder& add(MetadataType key, T&& value) & {
    builder_.add_vector(static_cast<count>(key), std::forward<T>(value));
    return *this;
  }

  template <class T>
  MetadataBuilder& add(count key, T&& value) & {
    builder_.add_vector(key, std::forward<T>(value));
    return *this;
  }

  template <class T>
  MetadataBuilder&& add(MetadataType key, T&& value) && {
    builder_.add_vector(static_cast<count>(key), std::forward<T>(value));
    return std::move(*this);
  }

  template <class T>
  MetadataBuilder&& add(count key, T&& value) && {
    builder_.add_vector(key, std::forward<T>(value));
    return std::move(*this);
  }

  const vector_builder& vec() const noexcept {
    return builder_;
  }

private:
  vector_builder builder_;
};

/// A builder object for constructing arguments to a Zeek event.
using ArgsBuilder = vector_builder;

/// A Zeek event.
class Event : public Message {
public:
  Event(Event&&) noexcept = default;

  Event(const Event&) noexcept = default;

  Event& operator=(Event&&) noexcept = default;

  Event& operator=(const Event&) noexcept = default;

  explicit Event(const data_view& msg) noexcept : Message(msg) {}

  static Event make(std::string_view name) {
    return Event{vector_builder{}
                   .add(ProtocolVersion)
                   .add(count{Type::Event})
                   .add(name)
                   .add_vector()
                   .build()};
  }

  /// Constructs an event.
  template <class... Ts>
  static Event make(std::string_view name,  Ts&&... xs) {
    return Event{vector_builder{}
                   .add(ProtocolVersion)
                   .add(count{Type::Event})
                   .add(name)
                   .add_vector(std::forward<Ts>(xs)...)
                   .build()};
  }

  /// Constructs an event with a network timestamp.
  template <class... Ts>
  static Event make_with_ts(std::string_view name, timestamp ts,  Ts&&... xs) {
    auto tag = static_cast<count>(MetadataType::NetworkTimestamp);
    return Event{vector_builder{}
                   .add(ProtocolVersion)
                   .add(count{Type::Event})
                   .add(name)
                   .add_vector(std::forward<Ts>(xs)...)
                   .add_vector(vector_builder{}.add(tag).add(ts))
                   .build()};
  }

  static Event make_with_args(std::string_view name, const ArgsBuilder& args) {
    return Event{vector_builder{}
                   .add(ProtocolVersion)
                   .add(count{Type::Event})
                   .add(name)
                   .add(args)
                   .build()};
  }

  static Event make_with_args(std::string_view name, const ArgsBuilder& args,
                              const MetadataBuilder& metadata) {
    return Event{vector_builder{}
                   .add(ProtocolVersion)
                   .add(count{Type::Event})
                   .add(name)
                   .add(args)
                   .add(metadata.vec())
                   .build()};
  }

  static Event convert_from(const data& src) {
    auto envelope = data_envelope::make(topic{"$"}, src);
    return Event{envelope->get_data()};
  }

  std::string_view name() const {
    return as_vector()[2].to_vector().front().to_string();
  }

  MetadataWrapper metadata() const {
    auto ev = as_vector()[2].to_vector();
    if(ev.size() < 3)
      return MetadataWrapper{data_view{}.to_vector()};
    return MetadataWrapper{ev[2].to_vector()};
  }

  std::optional<timestamp> ts() const {
    if (auto val = metadata().value(MetadataType::NetworkTimestamp);
        val.is_timestamp())
      return val.to_timestamp();
    return std::nullopt;
  }

  vector_view args() const {
    return as_vector()[2].to_vector()[1].to_vector();
  }

  bool valid() const {
    auto vec = as_vector();
    if (vec.size() < 3)
      return false;

    auto nested = vec[2].to_vector();
    if (nested.size() < 2)
      return false;

    if (!nested[0].is_string() || !nested[1].is_vector())
      return false;

    // Optional event metadata verification.
    //
    // Verify the third element if it exists is a vector<vector<count, data>>
    // and type and further check that the NetworkTimestamp metadata has the
    // right type because we know down here what to expect.
    if (nested.size() > 2) {
      auto meta_args = nested[2];
      if (!meta_args.is_vector())
        return false;
      for (const auto& meta_arg : meta_args.to_vector()) {
        auto meta = meta_arg.to_vector();
        if (meta.size() != 2)
          return false;
        if (!meta.front().is_count())
          return false;
        constexpr auto net_ts_key =
          static_cast<count>(MetadataType::NetworkTimestamp);
        if (meta.front().to_count() == net_ts_key
            && !meta.back().is_timestamp())
          return false;
      }
    }

    return true;
  }
};

/// A batch of other messages.
class Batch : public Message {
public:
  //Batch(vector msgs) : Message(Message::Type::Batch, std::move(msgs)) {}

  explicit Batch(data_view msg) : Message(std::move(msg)) {}

  vector_view batch() const {
    return as_vector()[2].to_vector();
  }

  bool valid() const {
    auto values = as_vector();
    if (values.size() < 3)
      return false;
    return values[2].is_vector();
  }
};

/*

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

*/

} // namespace broker::zeek
