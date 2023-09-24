#pragma once

#include <cstddef>
#include <iterator>
#include <optional>
#include <string>
#include <string_view>

#include "broker/builder.hh"
#include "broker/data.hh"
#include "broker/detail/assert.hh"
#include "broker/message.hh"
#include "broker/variant.hh"
#include "broker/variant_list.hh"

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

  virtual ~Message();

  Type type() const {
    return type(elements_);
  }

  data move_data() {
    return elements_.to_data();
  }

  /*
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
  */

  variant as_variant() const {
    return elements_;
  }

  data as_data() const {
    return elements_.to_data();
  }

  static Type type(const data& msg) {
    auto vp = get_if<vector>(&msg);

    if (!vp)
      return Type::Invalid;

    return type(*vp);
  }

  static Type type(const vector& v) {
    if (v.size() < 2)
      return Type::Invalid;

    auto cp = get_if<count>(&v[1]);

    if (!cp)
      return Type::Invalid;

    if (*cp > Type::MAX)
      return Type::Invalid;

    return Type(*cp);
  }

  static Type type(const variant& msg) {
    return type(msg.to_list());
  }

  static Type type(const variant_list& xs) {
    if (xs.size() < 2)
      return Type::Invalid;

    auto type_field = xs[1].to_count(Type::MAX + 1);
    if (type_field > Type::MAX)
      return Type::Invalid;

    return static_cast<Type>(type_field);
  }

  static Type type(const data_message& msg) {
    return type(msg->value());
  }

protected:
  /*
  Message(Type type, vector content)
    : data_(vector{ProtocolVersion, count(type), std::move(content)}) {}

  Message(data msg) : data_(std::move(msg)) {}
  */

  Message() = default;

  explicit Message(variant msg) : elements_(std::move(msg)) {}

  variant elements_; // Contains a list for valid messages.
};

class Invalid : public Message {
public:
  Invalid() = default;

  explicit Invalid(variant msg) : Message(std::move(msg)) {}
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
    auto [key, val] = pos_->to_list().take<2>();
    return {key.to_count(), val};
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
  explicit MetadataWrapper(variant_list items) noexcept : items_(items) {
    // nop
  }

  [[nodiscard]] MetadataIterator begin() const noexcept {
    return MetadataIterator{items_.begin()};
  }

  [[nodiscard]] MetadataIterator end() const noexcept {
    return MetadataIterator{items_.end()};
  }

  variant value(MetadataType key) const {
    return value(static_cast<count>(key));
  }

  variant value(count key) const {
    for (const auto& [k, v] : *this) {
      if (k == key)
        return v;
    }
    return {};
  }

  /// Raw access to the underlying metadata vector.
  const variant_list& raw() const noexcept {
    return items_;
  }

private:
  variant_list items_;
};

/// A Zeek event.
class Event : public Message {
public:
  explicit Event(variant msg) : Message(std::move(msg)) {}

  explicit Event(data_message msg) : Message(msg->value()) {}

  Event(std::string_view name, const vector& args) {
    elements_ = list_builder{}
                  .add(ProtocolVersion)
                  .add(static_cast<count>(Message::Type::Event))
                  .add_list(name, args)
                  .build();
  }

  Event(std::string_view name, const vector& args, timestamp ts) {
    list_builder meta;
    meta.add(static_cast<count>(MetadataType::NetworkTimestamp)).add(ts);
    elements_ = list_builder{}
                  .add(ProtocolVersion)
                  .add(static_cast<count>(Message::Type::Event))
                  .add_list(name, args, meta)
                  .build();
  }

  Event(std::string_view name, const vector& args, const vector& meta) {
    elements_ = list_builder{}
                  .add(ProtocolVersion)
                  .add(static_cast<count>(Message::Type::Event))
                  .add_list(name, args, meta)
                  .build();
  }

  /*
  Event(std::string_view name, variant_list args, timestamp ts) {
    list_builder meta;
    meta.add(static_cast<count>(MetadataType::NetworkTimestamp)).add(ts);
    elements_ = list_builder{}
                  .add(ProtocolVersion)
                  .add(static_cast<count>(Message::Type::Event))
                  .add_list(name, args, meta)
                  .build();
  }

  Event(std::string_view name, variant_list args, variant_list meta) {
    elements_ = list_builder{}
                  .add(ProtocolVersion)
                  .add(static_cast<count>(Message::Type::Event))
                  .add_list(name, args, meta)
                  .build();
  }
  */

  std::string_view name() const {
    return args().front().to_string();
  }

  variant_list args() const {
    return elements_.to_list().at(2).to_list();
  }

  bool valid() const {
    auto outer = elements_.to_list();
    if (outer.size() < 3)
      return false;

    auto inner = outer[2].to_list();

    if (inner.size() < 2)
      return false;

    auto [name, args] = inner.take<2>();

    if (!name.is_string() || !args.is_list())
      return false;

    // Optional event metadata verification.
    //
    // Verify the third element if it exists is a vector<vector<count, data>>
    // and type and further check that the NetworkTimestamp metadata has the
    // right type because we know down here what to expect.
    if (inner.size() > 2) {
      auto mde_var = inner[2];
      if (!mde_var.is_list())
        return false;

      for (const auto& mde : mde_var.to_list()) {
        auto mdev = mde.to_list();
        if (mdev.size() != 2)
          return false;

        auto [key,val]  = mdev.take<2>();

        if (!key.is_count())
          return false;

        constexpr auto net_ts_key =
          static_cast<count>(MetadataType::NetworkTimestamp);
        if (key.to_count() == net_ts_key && !val.is_timestamp())
          return false;
      }
    }
    return true;
  }

  template <class... Ts>
  static Event make(std::string_view name, const Ts&... elements) {
    static_assert(sizeof...(Ts) > 0);
    list_builder inner;
    (inner.add(elements), ...);
    auto res = list_builder{}
                 .add(ProtocolVersion)
                 .add(static_cast<count>(Message::Type::Event))
                 .add_list(name, std::move(inner))
                 .build();
    return Event{res};
  }

  MetadataWrapper metadata() const {
    return MetadataWrapper{elements_.to_list().at(2).to_list()};
  }

  std::optional<timestamp> ts() const {
    auto val = metadata().value(MetadataType::NetworkTimestamp);
    if (val.is_timestamp())
      return val.to_timestamp();
    return std::nullopt;
  }

  /*
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
  */
};

/// A Zeek log-create message. Note that at the moment this should be used
/// only by Zeek itself as the arguments aren't pulbically defined.
class LogCreate : public Message {
public:
  explicit LogCreate(variant elements) : Message(std::move(elements)) {}

  explicit LogCreate(data_message msg) : LogCreate(msg->value()) {}

  LogCreate(const enum_value& stream_id, const enum_value& writer_id,
            const data& writer_info, const data& fields_data) {
    elements_ = list_builder{}
                  .add(ProtocolVersion)
                  .add(static_cast<count>(Message::Type::LogCreate))
                  .add_list(stream_id, writer_id, writer_info, fields_data)
                  .build();
  }

  /*
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
  */

  enum_value_view stream_id() const {
    return elements_.to_list().at(2).to_list().at(0).to_enum_value();
  }

  enum_value_view writer_id() const {
    return elements_.to_list().at(2).to_list().at(1).to_enum_value();
  }

  variant_list writer_info() const {
    return elements_.to_list().at(2).to_list().at(2).to_list();
  }

  variant_list fields() const {
    return elements_.to_list().at(2).to_list().at(3).to_list();
  }

  bool valid() const {
    auto elements = elements_.to_list();
    if (elements.size() < 3)
      return false;
    auto inner = elements.at(2).to_list();
    return inner.size() >= 4              //
           && inner.at(0).is_enum_value() //
           && inner.at(1).is_enum_value() //
           && inner.at(2).is_list()       //
           && inner.at(3).is_list();
  }
};

/// A Zeek log-write message. Note that at the moment this should be used only
/// by Zeek itself as the arguments aren't publicly defined.
class LogWrite : public Message {
public:
  explicit LogWrite(variant elements) : Message(std::move(elements)) {}

  explicit LogWrite(data_message msg) : LogWrite(msg->value()) {}

  LogWrite(const enum_value& stream_id, const enum_value& writer_id,
           const data& path, const data& serial_data) {
    elements_ = list_builder{}
                  .add(ProtocolVersion)
                  .add(static_cast<count>(Message::Type::LogWrite))
                  .add_list(stream_id, writer_id, path, serial_data)
                  .build();
  }

  /*
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
  */

  enum_value_view stream_id() const {
    return elements_.to_list().at(2).to_list().at(0).to_enum_value();
  }

  enum_value_view writer_id() const {
    return elements_.to_list().at(2).to_list().at(1).to_enum_value();
  }

  std::string_view path() const {
    return elements_.to_list().at(2).to_list().at(2).to_string();
  }

  std::string_view serial_data() const {
    return elements_.to_list().at(2).to_list().at(3).to_string();
  }

  bool valid() const {
    auto elements = elements_.to_list();
    if (elements.size() < 3)
      return false;
    auto inner = elements.at(2).to_list();
    return inner.size() >= 4              //
           && inner.at(0).is_enum_value() //
           && inner.at(1).is_enum_value() //
           && inner.at(2).is_string()     //
           && inner.at(3).is_string();
  }
};

class IdentifierUpdate : public Message {
public:
  explicit IdentifierUpdate(variant elements) : Message(std::move(elements)) {}

  explicit IdentifierUpdate(data_message msg)
    : IdentifierUpdate(msg->value()) {}

  template <class Data, class = std::enable_if_t<std::is_same_v<Data, data>>>
  IdentifierUpdate(std::string_view id_name, const Data& id_value) {
    // Note: this constructor only a template to "disable" the nasty implicit
    //       conversions of broker::data.
    elements_ = list_builder{}
                  .add(ProtocolVersion)
                  .add(static_cast<count>(Message::Type::IdentifierUpdate))
                  .add_list(id_name, id_value)
                  .build();
  }

  /*
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
  */

  std::string_view id_name() const {
    return elements_.to_list().at(2).to_list().at(0).to_string();
  }

  variant id_value() const {
    return elements_.to_list().at(2).to_list().at(1);
  }

  bool valid() const {
    auto elements = elements_.to_list();
    if (elements.size() < 3)
      return false;
    auto inner = elements.at(2).to_list();
    return inner.size() >= 2 && inner.at(0).is_string();
  }
};

class BatchBuilder;

/// A batch of other messages.
class Batch : public Message {
public:
  friend class BatchBuilder;

  explicit Batch(variant elements);

  explicit Batch(data_message msg) : Batch(msg->value()) {}

  Batch(Batch&&) = default;

  Batch(const Batch&) = default;

  Batch& operator=(Batch&&) = default;

  Batch& operator=(const Batch&) = default;

  /*
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
  */

  bool size() const noexcept {
    return impl_ ? impl_->size() : 0;
  }

  bool valid() const noexcept {
    return impl_ != nullptr;
  }

  const Message& at(size_t index) const {
    return std::visit([](const auto& msg) -> const Message& { return msg; },
                      impl_->at(index));
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

  explicit Batch(list_builder&& items);

  std::shared_ptr<Content> impl_;
};

class BatchBuilder {
public:
  void add(const Message& msg) {
    inner_.add(msg.as_variant());
  }

  bool empty() const noexcept {
    return inner_.num_values() == 0;
  }

  Batch build() {
    auto result = Batch{std::move(inner_)};
    inner_.reset();
    return result;
  }

private:
  list_builder inner_;
};

template <class F>
auto visit_as_message(F&& f, const broker::variant& msg) {
  switch (Message::type(msg)) {
    default:
      break;
    case Message::Type::Event: {
      Event tmp{msg};
      if (tmp.valid())
        return f(tmp);
      break;
    }
    case Message::Type::LogCreate: {
      LogCreate tmp{msg};
      if (tmp.valid())
        return f(tmp);
      break;
    }
    case Message::Type::LogWrite: {
      LogWrite tmp{msg};
      if (tmp.valid())
        return f(tmp);
      break;
    }
    case Message::Type::IdentifierUpdate: {
      IdentifierUpdate tmp{msg};
      if (tmp.valid())
        return f(tmp);
      break;
    }
    case Message::Type::Batch: {
      Batch tmp{msg};
      if (tmp.valid())
        return f(tmp);
      break;
    }
  }
  Invalid tmp{msg};
  return f(tmp);
}

} // namespace broker::zeek

namespace broker {

inline std::string to_string(const zeek::Message& msg) {
  return to_string(msg.as_variant());
}

} // namespace broker
