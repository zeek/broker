#pragma once

#include <cstddef>
#include <iterator>
#include <optional>

#include "broker/data.hh"
#include "broker/detail/assert.hh"
#include "broker/message.hh"

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
  /// The index of the version field in the message.
  static constexpr size_t version_index = 0;

  /// The index of the type field in the message.
  static constexpr size_t type_index = 1;

  /// The index of the content field in the message. The type of the content
  /// depends on the sub-type of the message.
  static constexpr size_t content_index = 2;

  /// The number of top-level fields in the message.
  static constexpr size_t num_top_level_fields = 3;

  enum Type {
    Invalid = 0,
    Event = 1,
    LogCreate = 2,
    LogWrite = 3,
    IdentifierUpdate = 4,
    Batch = 5,
    MAX = Batch,
  };

  static constexpr auto max_tag = static_cast<count>(Type::MAX);

  virtual ~Message();

  Type type() const {
    return type(data_);
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

  static Type type(const data& msg) {
    auto&& elements = msg.to_list();
    if (elements.size() >= num_top_level_fields) {
      auto tag = elements[type_index].to_count();
      if (tag <= max_tag) {
        return static_cast<Type>(tag);
      }
    }
    return Type::Invalid;
  }

  static Type type(const data_message& msg) {
    return type(get_data(msg));
  }

protected:
  bool validate_outer_fields(Type tag) const {
    auto&& outer = data_.to_list();
    if (outer.size() < num_top_level_fields)
      return false;

    return outer[version_index].to_count() == ProtocolVersion
           && outer[type_index].to_count() == static_cast<count>(tag)
           && outer[content_index].is_list();
  }

  /// Returns the content of the message, i.e., the fields for the sub-type.
  /// @pre validate_outer_fields(tag)
  const vector& sub_fields() const {
    auto&& outer = data_.to_list();
    return outer[content_index].to_list();
  }

  /// @copydoc sub_fields()
  vector& sub_fields() {
    auto& outer = as_vector();
    return get<vector>(outer[content_index]);
  }

  explicit Message(Type type, vector content)
    : data_(vector{ProtocolVersion, count(type), std::move(content)}) {}

  explicit Message(data msg) : data_(std::move(msg)) {}

  Message() = default;

  Message(Message&&) = default;

  data data_;
};

/// Represents an invalid message.
class Invalid : public Message {
public:
  Invalid() = default;

  explicit Invalid(data msg) : Message(std::move(msg)) {}

  explicit Invalid(data_message msg) : Invalid(broker::move_data(msg)) {}

  explicit Invalid(Message&& msg) : Message(std::move(msg)) {}
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
  /// The index of the event name field.
  static constexpr size_t name_index = 0;

  /// The index of the event arguments field.
  static constexpr size_t args_index = 1;

  /// The index of the optional metadata field.
  static constexpr size_t metadata_index = 2;

  /// The minimum number of fields in a valid event.
  static constexpr size_t min_fields = 2;

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

  explicit Event(data msg) : Message(std::move(msg)) {}

  explicit Event(data_message msg) : Event(broker::move_data(msg)) {}

  const std::string& name() const {
    auto&& fields = sub_fields();
    return get<std::string>(fields[name_index]);
  }

  std::string& name() {
    auto&& fields = sub_fields();
    return get<std::string>(fields[name_index]);
  }

  MetadataWrapper metadata() const {
    auto&& fields = sub_fields();
    if (fields.size() > metadata_index)
      return MetadataWrapper{get_if<vector>(fields[metadata_index])};

    return MetadataWrapper{nullptr};
  }

  const std::optional<timestamp> ts() const {
    if (auto ts_ptr = metadata().value(MetadataType::NetworkTimestamp))
      return get<timestamp>(*ts_ptr);

    return std::nullopt;
  }

  const vector& args() const {
    auto&& fields = sub_fields();
    return get<vector>(fields[args_index]);
  }

  vector& args() {
    auto&& fields = sub_fields();
    return get<vector>(fields[args_index]);
  }

  bool valid() const {
    if (!validate_outer_fields(Type::Event))
      return false;

    auto&& fields = sub_fields();

    if (fields.size() < min_fields || !fields[name_index].is_string()
        || !fields[args_index].is_list())
      return false;

    // Optional event metadata verification.
    //
    // Verify the third element if it exists is a vector<vector<count, data>>
    // and type and further check that the NetworkTimestamp metadata has the
    // right type because we know down here what to expect.
    if (fields.size() > metadata_index) {
      auto&& meta_field = fields[metadata_index];
      if (!meta_field.is_list())
        return false;

      for (const auto& field : meta_field.to_list()) {
        auto&& kvp = field.to_list();

        // Must be two elements: key and value.
        if (kvp.size() != 2 || !kvp[0].is_count())
          return false;

        // If we have a NetworkTimestamp key, the value must be a timestamp.
        constexpr auto net_ts_key =
          static_cast<count>(MetadataType::NetworkTimestamp);
        if (kvp[0].to_count() == net_ts_key && !kvp[1].is_timestamp())
          return false;
      }
    }

    return true;
  }
};

/// A Zeek log-create message. Note that at the moment this should be used
/// only by Zeek itself as the arguments aren't pulbically defined.
class LogCreate : public Message {
public:
  /// The index of the stream ID field.
  static constexpr size_t stream_id_index = 0;

  /// The index of the writer ID field.
  static constexpr size_t writer_id_index = 1;

  /// The index of the writer info field.
  static constexpr size_t writer_info_index = 2;

  /// The index of the fields data field.
  static constexpr size_t fields_data_index = 3;

  /// The minimum number of fields in a valid log-create message.
  static constexpr size_t min_fields = 4;

  LogCreate(enum_value stream_id, enum_value writer_id, data writer_info,
            data fields_data)
    : Message(Message::Type::LogCreate,
              {std::move(stream_id), std::move(writer_id),
               std::move(writer_info), std::move(fields_data)}) {}

  explicit LogCreate(data msg) : Message(std::move(msg)) {}

  explicit LogCreate(data_message msg) : LogCreate(broker::move_data(msg)) {}

  const enum_value& stream_id() const {
    auto&& fields = sub_fields();
    return get<enum_value>(fields[stream_id_index]);
  }

  enum_value& stream_id() {
    auto&& fields = sub_fields();
    return get<enum_value>(fields[stream_id_index]);
  }

  const enum_value& writer_id() const {
    auto&& fields = sub_fields();
    return get<enum_value>(fields[writer_id_index]);
  }

  enum_value& writer_id() {
    auto&& fields = sub_fields();
    return get<enum_value>(fields[writer_id_index]);
  }

  const data& writer_info() const {
    auto&& fields = sub_fields();
    return fields[writer_info_index];
  }

  data& writer_info() {
    auto&& fields = sub_fields();
    return fields[writer_info_index];
  }

  const data& fields_data() const {
    auto&& fields = sub_fields();
    return fields[fields_data_index];
  }

  data& fields_data() {
    auto&& fields = sub_fields();
    return fields[fields_data_index];
  }

  bool valid() const {
    if (!validate_outer_fields(Type::LogCreate))
      return false;

    auto&& fields = sub_fields();
    return fields.size() >= min_fields
           && fields[stream_id_index].is_enum_value()
           && fields[writer_id_index].is_enum_value();
  }
};

/// A Zeek log-write message. Note that at the moment this should be used only
/// by Zeek itself as the arguments aren't publicly defined.
class LogWrite : public Message {
public:
  /// The index of the stream ID field.
  static constexpr size_t stream_id_index = 0;

  /// The index of the writer ID field.
  static constexpr size_t writer_id_index = 1;

  /// The index of the path field.
  static constexpr size_t path_index = 2;

  /// The index of the serial data field.
  static constexpr size_t serial_data_index = 3;

  /// The minimum number of fields in a valid log-create message.
  static constexpr size_t min_fields = 4;

  LogWrite(enum_value stream_id, enum_value writer_id, data path,
           data serial_data)
    : Message(Message::Type::LogWrite,
              {std::move(stream_id), std::move(writer_id), std::move(path),
               std::move(serial_data)}) {}

  explicit LogWrite(data msg) : Message(std::move(msg)) {}

  explicit LogWrite(data_message msg) : LogWrite(broker::move_data(msg)) {}

  const enum_value& stream_id() const {
    auto&& fields = sub_fields();
    return get<enum_value>(fields[stream_id_index]);
  }

  enum_value& stream_id() {
    auto&& fields = sub_fields();
    return get<enum_value>(fields[stream_id_index]);
  }

  const enum_value& writer_id() const {
    auto&& fields = sub_fields();
    return get<enum_value>(fields[writer_id_index]);
  }

  enum_value& writer_id() {
    auto&& fields = sub_fields();
    return get<enum_value>(fields[writer_id_index]);
  }

  const data& path() const {
    auto&& fields = sub_fields();
    return fields[path_index];
  }

  data& path() {
    auto&& fields = sub_fields();
    return fields[path_index];
  };

  std::string_view path_str() {
    auto&& fields = sub_fields();
    return fields[path_index].to_string();
  };

  const data& serial_data() const {
    auto&& fields = sub_fields();
    return fields[serial_data_index];
  }

  data& serial_data() {
    auto&& fields = sub_fields();
    return fields[serial_data_index];
  }

  std::string_view serial_data_str() const {
    auto&& fields = sub_fields();
    return fields[serial_data_index].to_string();
  }

  bool valid() const {
    if (!validate_outer_fields(Type::LogWrite))
      return false;

    auto&& fields = sub_fields();
    return fields.size() >= min_fields
           && fields[stream_id_index].is_enum_value()
           && fields[writer_id_index].is_enum_value()
           && fields[path_index].is_string()
           && fields[serial_data_index].is_string();
  }
};

class IdentifierUpdate : public Message {
public:
  /// The index of the ID name field.
  static constexpr size_t id_name_index = 0;

  /// The index of the ID value field.
  static constexpr size_t id_value_index = 1;

  /// The minimum number of fields in a valid identifier-update message.
  static constexpr size_t min_fields = 2;

  IdentifierUpdate(std::string id_name, data id_value)
    : Message(Message::Type::IdentifierUpdate,
              {std::move(id_name), std::move(id_value)}) {}

  explicit IdentifierUpdate(data msg) : Message(std::move(msg)) {}

  explicit IdentifierUpdate(data_message msg)
    : IdentifierUpdate(broker::move_data(msg)) {}

  const std::string& id_name() const {
    auto&& fields = sub_fields();
    return get<std::string>(fields[id_name_index]);
  }

  std::string& id_name() {
    auto&& fields = sub_fields();
    return get<std::string>(fields[id_name_index]);
  }

  const data& id_value() const {
    auto&& fields = sub_fields();
    return fields[id_value_index];
  }

  data& id_value() {
    auto&& fields = sub_fields();
    return fields[id_value_index];
  }

  bool valid() const {
    if (!validate_outer_fields(Type::IdentifierUpdate))
      return false;

    auto&& fields = sub_fields();
    return fields.size() >= min_fields && fields[id_name_index].is_string();
  }
};

class BatchBuilder;

/// A batch of other messages.
class Batch : public Message {
public:
  explicit Batch(data msg);

  explicit Batch(data_message msg) : Batch(broker::move_data(msg)) {}

  size_t size() const noexcept {
    return impl_ ? impl_->size() : 0;
  }

  bool empty() const noexcept {
    return size() == 0;
  }

  bool valid() const {
    return impl_ != nullptr;
  }

  template <class F>
  auto for_each(F&& f) {
    if (!impl_)
      return;
    for (auto& x : *impl_)
      std::visit(f, x);
  }

  template <class F>
  auto for_each(F&& f) const {
    if (!impl_)
      return;
    for (const auto& x : *impl_)
      std::visit(f, x);
  }

private:
  using VarMsg =
    std::variant<broker::zeek::Event, broker::zeek::LogCreate,
                 broker::zeek::LogWrite, broker::zeek::IdentifierUpdate,
                 broker::zeek::Batch>;

  using Content = std::vector<VarMsg>;

  std::shared_ptr<Content> impl_;
};

class BatchBuilder {
public:
  void add(Message&& msg) {
    inner_.emplace_back(msg.move_data());
  }

  bool empty() const noexcept {
    return inner_.empty();
  }

  Batch build();

private:
  vector inner_;
};

template <class F>
auto visit_as_message(F&& f, broker::data_message msg) {
  auto do_visit = [&f](auto& tmp) {
    if (tmp.valid())
      return f(tmp);
    Invalid fallback{std::move(tmp)};
    return f(fallback);
  };
  switch (Message::type(msg)) {
    default: {
      Invalid tmp{std::move(msg)};
      return f(tmp);
    }
    case Message::Type::Event: {
      Event tmp{std::move(msg)};
      return do_visit(tmp);
    }
    case Message::Type::LogCreate: {
      LogCreate tmp{std::move(msg)};
      return do_visit(tmp);
    }
    case Message::Type::LogWrite: {
      LogWrite tmp{std::move(msg)};
      return do_visit(tmp);
    }
    case Message::Type::IdentifierUpdate: {
      IdentifierUpdate tmp{std::move(msg)};
      return do_visit(tmp);
    }
    case Message::Type::Batch: {
      Batch tmp{std::move(msg)};
      return do_visit(tmp);
    }
  }
}

} // namespace broker::zeek

namespace broker {

inline std::string to_string(const zeek::Message& msg) {
  return to_string(msg.as_data());
}

} // namespace broker
