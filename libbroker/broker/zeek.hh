#pragma once

#include <cstddef>
#include <iterator>
#include <optional>

#include "broker/builder.hh"
#include "broker/data.hh"
#include "broker/detail/assert.hh"
#include "broker/message.hh"
#include "broker/variant.hh"
#include "broker/variant_list.hh"

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

class BatchBuilder;

/// Generic Zeek-level message.
class Message {
public:
  friend class BatchBuilder;

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

  static Type type(const variant& msg) {
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

  std::string to_string() const {
    return broker::to_string(data_);
  }

  /// Returns the underlying data as-is.
  const variant& raw() const noexcept {
    return data_;
  }

  /// Returns the underlying data as-is.
  const variant& as_data() const noexcept {
    // Exists only for backwards compatibility.
    return data_;
  }

  /// Returns the underlying data as-is.
  variant move_data() const noexcept {
    // TODO: this isn't moving anything, really. Should be deprecated/removed.
    return data_;
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
  variant_list sub_fields() const {
    auto&& outer = data_.to_list();
    return outer[content_index].to_list();
  }

  void init(Type sub_type, const list_builder& content);

  explicit Message(variant msg) : data_(std::move(msg)) {}

  Message() = default;

  Message(Message&&) = default;

  variant data_;
};

/// Represents an invalid message.
class Invalid : public Message {
public:
  Invalid() = default;

  explicit Invalid(variant msg) : Message(std::move(msg)) {}

  explicit Invalid(const data_message& msg) : Invalid(broker::get_data(msg)) {}

  explicit Invalid(Message&& msg) : Message(std::move(msg)) {}
};

/// Support iteration with structured binding.
class MetadataIterator {
public:
  using iterator_category = std::forward_iterator_tag;
  using difference_type = std::ptrdiff_t;
  using value_type = std::pair<count, variant>;
  using pointer = value_type*;
  using reference = value_type&;

  explicit MetadataIterator(variant_list::iterator pos) noexcept : pos_(pos) {}

  MetadataIterator(const MetadataIterator&) noexcept = default;

  MetadataIterator& operator=(const MetadataIterator&) noexcept = default;

  value_type operator*() const {
    auto entry = pos_->to_list();
    BROKER_ASSERT(entry.size() == 2);
    return {entry[0].to_count(), entry[1]};
  }

  MetadataIterator& operator++() noexcept {
    ++pos_;
    return *this;
  }

  MetadataIterator operator++(int) noexcept {
    return MetadataIterator{pos_++};
  }

  bool operator!=(const MetadataIterator& other) const noexcept {
    return pos_ != other.pos_;
  }

  bool operator==(const MetadataIterator& other) const noexcept {
    return pos_ == other.pos_;
  }

private:
  variant_list::iterator pos_;
};

/// Supports iteration over metadata
class MetadataWrapper {
public:
  MetadataWrapper() noexcept = default;

  explicit MetadataWrapper(variant_list v) noexcept : v_(std::move(v)) {}

  [[nodiscard]] MetadataIterator begin() const noexcept {
    return MetadataIterator{v_.begin()};
  }

  [[nodiscard]] MetadataIterator end() const noexcept {
    return MetadataIterator{v_.end()};
  }

  [[nodiscard]] variant value(count key) const {
    for (const auto& [k, v] : *this) {
      if (k == key)
        return v;
    }
    return {};
  }

  [[nodiscard]] auto value(MetadataType key) const {
    return value(static_cast<count>(key));
  }

  [[nodiscard]] size_t size() const noexcept {
    return v_.size();
  }

  [[nodiscard]] bool empty() const noexcept {
    return v_.empty();
  }

  [[nodiscard]] const variant_list& raw() const noexcept {
    return v_;
  }

private:
  variant_list v_;
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

  Event(std::string_view name, const vector& args) {
    init(Type::Event, list_builder{}.add(name).add(args));
  }

  Event(std::string_view name, const vector& args, timestamp ts) {
    list_builder meta;
    meta.add_list(static_cast<count>(MetadataType::NetworkTimestamp), ts);
    init(Type::Event, list_builder{}.add(name).add(args).add(meta));
  }

  Event(std::string_view name, const list_builder& args) {
    init(Type::Event, list_builder{}.add(name).add(args));
  }

  Event(std::string_view name, const list_builder& args, timestamp ts) {
    list_builder meta;
    meta.add_list(static_cast<count>(MetadataType::NetworkTimestamp), ts);
    init(Type::Event, list_builder{}.add(name).add(args).add(meta));
  }

  Event(std::string_view name, const vector& args, const vector& meta) {
    init(Type::Event, list_builder{}.add(name).add(args).add(meta));
  }

  Event(std::string_view name, const list_builder& args,
        const list_builder& meta) {
    init(Type::Event, list_builder{}.add(name).add(args).add(meta));
  }

  explicit Event(variant msg) : Message(std::move(msg)) {}

  explicit Event(const data_message& msg) : Message(broker::get_data(msg)) {}

  std::string_view name() const {
    auto&& fields = sub_fields();
    return fields[name_index].to_string();
  }

  MetadataWrapper metadata() const {
    auto&& fields = sub_fields();
    if (fields.size() > metadata_index)
      return MetadataWrapper{fields[metadata_index].to_list()};
    return MetadataWrapper{};
  }

  const std::optional<timestamp> ts() const {
    if (auto ts = metadata().value(MetadataType::NetworkTimestamp);
        ts.is_timestamp())
      return ts.to_timestamp();

    return std::nullopt;
  }

  variant_list args() const {
    auto&& fields = sub_fields();
    return fields[args_index].to_list();
  }

  bool valid() const {
    if (!validate_outer_fields(Type::Event)) {
      puts("outer fields invalid");
      return false;
    }

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

  template <class WriterInfo, class FieldsData>
  LogCreate(const enum_value& stream_id, const enum_value& writer_id,
            const WriterInfo& writer_info, const FieldsData& fields_data) {
    init(Message::Type::LogCreate, list_builder{}
                                     .add(stream_id)
                                     .add(writer_id)
                                     .add(writer_info)
                                     .add(fields_data));
  }

  explicit LogCreate(variant msg) : Message(std::move(msg)) {}

  explicit LogCreate(const data_message& msg)
    : LogCreate(broker::move_data(msg)) {}

  enum_value_view stream_id() const {
    auto&& fields = sub_fields();
    return fields[stream_id_index].to_enum_value();
  }

  enum_value_view writer_id() const {
    auto&& fields = sub_fields();
    return fields[writer_id_index].to_enum_value();
  }

  variant writer_info() const {
    auto&& fields = sub_fields();
    return fields[writer_info_index];
  }

  variant fields_data() const {
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

  LogWrite(const enum_value& stream_id, const enum_value& writer_id,
           std::string_view path, std::string_view serial_data) {
    init(Message::Type::LogWrite, list_builder{} //
                                    .add(stream_id)
                                    .add(writer_id)
                                    .add(path)
                                    .add(serial_data));
  }

  explicit LogWrite(variant msg) : Message(std::move(msg)) {}

  explicit LogWrite(const data_message& msg)
    : LogWrite(broker::move_data(msg)) {}

  enum_value_view stream_id() const {
    auto&& fields = sub_fields();
    return fields[stream_id_index].to_enum_value();
  }

  enum_value_view writer_id() const {
    auto&& fields = sub_fields();
    return fields[writer_id_index].to_enum_value();
  }

  std::string_view path_str() {
    auto&& fields = sub_fields();
    return fields[path_index].to_string();
  };

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

  template <class VariantOrData>
  IdentifierUpdate(std::string id_name, const VariantOrData& id_value) {
    init(Type::IdentifierUpdate, list_builder{}.add(id_name).add(id_value));
  }

  explicit IdentifierUpdate(variant msg) : Message(std::move(msg)) {}

  explicit IdentifierUpdate(const data_message& msg)
    : IdentifierUpdate(broker::move_data(msg)) {}

  std::string_view id_name() const {
    auto&& fields = sub_fields();
    return fields[id_name_index].to_string();
  }

  variant id_value() const {
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

/// A batch of other messages.
class Batch : public Message {
public:
  explicit Batch(variant msg);

  explicit Batch(const data_message& msg) : Batch(broker::get_data(msg)) {}

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
    inner_.add(msg.data_);
  }

  bool empty() const noexcept {
    return inner_.empty();
  }

  Batch build();

private:
  list_builder inner_;
};

template <class F>
auto visit_as_message(F&& f, const broker::data_message& msg) {
  auto do_visit = [&f](auto& tmp) {
    if (tmp.valid())
      return f(tmp);
    Invalid fallback{std::move(tmp)};
    return f(fallback);
  };
  switch (Message::type(msg)) {
    default: {
      Invalid tmp{msg};
      return f(tmp);
    }
    case Message::Type::Event: {
      Event tmp{msg};
      return do_visit(tmp);
    }
    case Message::Type::LogCreate: {
      LogCreate tmp{msg};
      return do_visit(tmp);
    }
    case Message::Type::LogWrite: {
      LogWrite tmp{msg};
      return do_visit(tmp);
    }
    case Message::Type::IdentifierUpdate: {
      IdentifierUpdate tmp{msg};
      return do_visit(tmp);
    }
    case Message::Type::Batch: {
      Batch tmp{msg};
      return do_visit(tmp);
    }
  }
}

} // namespace broker::zeek

namespace broker {

inline std::string to_string(const zeek::Message& msg) {
  return msg.to_string();
}

} // namespace broker
