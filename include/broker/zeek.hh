#pragma once

#include "broker/data.hh"

namespace broker {
namespace zeek {

const count ProtocolVersion = 1;

/// Generic Zeek-level message.
class Message {
public:
  enum Type {
    Invalid = 0,
    Event = 1,
    LogCreate = 2,
    LogWrite = 3,
    IdentifierUpdate = 4,
    MAX = IdentifierUpdate,
  };

  Type type() const {
    if ( as_vector().size() < 2 )
      return Type::Invalid;

    auto cp = caf::get_if<count>(&as_vector()[1]);

    if ( ! cp )
      return Type::Invalid;

    if ( *cp > Type::MAX )
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
    return caf::get<vector>(data_);
  }

  vector& as_vector() {
    return caf::get<vector>(data_);
   }

  const vector& content() const {
    return caf::get<vector>(caf::get<vector>(data_)[2]);
  }

  vector& content() {
    return caf::get<vector>(caf::get<vector>(data_)[2]);
  }

  static Type type(const data& msg) {
    auto vp = caf::get_if<vector>(&msg);

    if ( ! vp )
      return Type::Invalid;

    auto& v = *vp;

    if ( v.size() < 2 )
      return Type::Invalid;

    auto cp = caf::get_if<count>(&v[1]);

    if ( ! cp )
      return Type::Invalid;

    if ( *cp > Type::MAX )
      return Type::Invalid;

    return Type(*cp);
  }

  static bool valid(const broker::data& raw) {
    if (auto vec = caf::get_if<vector>(&raw)) {
      if (vec->size() < 3)
        return false;
      if (auto version = caf::get_if<count>(&(*vec)[0])) {
        if (*version != ProtocolVersion)
          return false;
      } else {
        return false;
      }
      if (auto type = caf::get_if<count>(&(*vec)[1])) {
        if (*type == 0 || *type > static_cast<count>(MAX))
          return false;
      } else {
        return false;
      }
      return caf::holds_alternative<vector>((*vec)[2]);
    }
    return false;
  }

protected:
  Message(Type type, vector content)
    : data_(vector{ProtocolVersion, count(type), std::move(content)}) {
  }

  explicit Message(data&& raw) : data_(std::move(raw)) {
    // nop
  }

  data data_;
};

/// A Zeek event.
class Event : public Message {
public:
  Event(std::string name, vector args)
    : Message(Message::Type::Event, {std::move(name), std::move(args)}) {}

  const std::string& name() const {
    return caf::get<std::string>(content()[0]);
  }

  std::string& name() {
    return caf::get<std::string>(content()[0]);
  }

  const vector& args() const {
    return caf::get<vector>(content()[1]);
  }

  vector& args() {
    return caf::get<vector>(content()[1]);
  }

  static bool valid(const broker::data& raw) noexcept {
    if (!Message::valid(raw))
      return false;
    auto& vec = caf::get<vector>(caf::get<vector>(raw)[2]);
    if (vec.size() != 2)
      return false;
    return caf::holds_alternative<std::string>(vec[0])
           && caf::holds_alternative<vector>(vec[1]);
  }

  /// @pre @c valid(raw)
  static Event from(data&& raw) {
    return Event{std::move(raw)};
  }

private:
  explicit Event(data&& raw) : Message(std::move(raw)) {
    // nop
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
               std::move(writer_info), std::move(fields_data)}) {
  }

  const enum_value& stream_id() const {
    return caf::get<enum_value>(content()[0]);
  }

  enum_value& stream_id() {
    return caf::get<enum_value>(content()[0]);
  }

  const enum_value& writer_id() const {
    return caf::get<enum_value>(content()[1]);
  }

  enum_value& writer_id() {
    return caf::get<enum_value>(content()[1]);
  }

  const data& writer_info() const {
    return content()[2];
  }

  data& writer_info() {
    return content()[2];
  }

  const data& fields_data() const {
    return content()[3];
  }

  data& fields_data() {
    return content()[3];
  }

  static bool valid(const data& raw) noexcept {
    if (!Message::valid(raw))
      return false;
    auto& vec = caf::get<vector>(caf::get<vector>(raw)[2]);
    if (vec.size() != 4)
      return false;
    return caf::holds_alternative<enum_value>(vec[0])
           && caf::holds_alternative<enum_value>(vec[1]);
  }

  /// @pre @c valid(raw)
  static LogCreate from(data&& raw) {
    return LogCreate{std::move(raw)};
  }

private:
  explicit LogCreate(data&& raw) : Message(std::move(raw)) {
    // nop
  }
};

/// A Zeek log-write message. Note that at the moment this should be used only
/// by Zeek itself as the arguments aren't publicly defined.
class LogWrite : public Message {
public:
  LogWrite(enum_value stream_id, enum_value writer_id, std::string path,
           vector values)
    : Message(Message::Type::LogWrite,
              {std::move(stream_id), std::move(writer_id),
               std::move(path), std::move(values)}) {
  }

  const enum_value& stream_id() const {
    return caf::get<enum_value>(content()[0]);
  }

  enum_value& stream_id() {
    return caf::get<enum_value>(content()[0]);
  }

  const enum_value& writer_id() const {
    return caf::get<enum_value>(content()[1]);
  }

  enum_value& writer_id() {
    return caf::get<enum_value>(content()[1]);
  }

  const std::string& path() const {
    return caf::get<std::string>(content()[2]);
  }

  std::string& path() {
    return caf::get<std::string>(content()[2]);
  };

  vector& values() {
    return caf::get<vector>(content()[3]);
  }

  const vector& values() const {
    return caf::get<vector>(content()[3]);
  }

  static bool valid(const data& raw) noexcept {
    if (!Message::valid(raw))
      return false;
    auto& vec = caf::get<vector>(caf::get<vector>(raw)[2]);
    if (vec.size() != 4)
      return false;
    return caf::holds_alternative<enum_value>(vec[0])
           && caf::holds_alternative<enum_value>(vec[1])
           && caf::holds_alternative<std::string>(vec[2])
           && caf::holds_alternative<vector>(vec[3]);
  }

  /// @pre @c valid(raw)
  static LogWrite from(data&& raw) {
    return LogWrite{std::move(raw)};
  }

private:
  explicit LogWrite(data&& raw) : Message(std::move(raw)) {
    // nop
  }
};

class IdentifierUpdate : public Message {
public:
  IdentifierUpdate(std::string id_name, data id_value)
    : Message(Message::Type::IdentifierUpdate, {std::move(id_name),
    		                                    std::move(id_value)}) {
  }

  const std::string& id_name() const {
    return caf::get<std::string>(content()[0]);
  }

  std::string& id_name() {
    return caf::get<std::string>(content()[0]);
  }

  const data& id_value() const {
    return content()[1];
  }

  data& id_value() {
    return content()[1];
  }

  static bool valid(const data& raw) noexcept {
    if (!Message::valid(raw))
      return false;
    auto& vec = caf::get<vector>(caf::get<vector>(raw)[2]);
    if (vec.size() != 2)
      return false;
    return caf::holds_alternative<std::string>(vec[0]);
  }


  /// @pre @c valid(raw)
  static IdentifierUpdate from(data&& raw) {
    return IdentifierUpdate{std::move(raw)};
  }

private:
  explicit IdentifierUpdate(data&& raw) : Message(std::move(raw)) {
    // nop
  }
};

} // namespace broker
} // namespace zeek
