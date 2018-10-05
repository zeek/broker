#ifndef BROKER_BRO_HH
#define BROKER_BRO_HH

#include "broker/data.hh"

namespace broker {
namespace bro {

const count ProtocolVersion = 1;

/// Generic Bro-level message.
class Message {
public:
  enum Type {
    Event = 1,
    LogCreate = 2,
    LogWrite = 3,
    IdentifierUpdate = 4,
    Batch = 5,
  };

  Type type() const {
    return Type(caf::get<count>(msg_[1]));
  }

  data as_data() const {
    return msg_;
  }

  operator data() const {
    return as_data();
  }

  static Type type(const data& msg) {
    return Type(caf::get<count>(caf::get<vector>(msg)[1]));
  }

protected:
  Message(Type type, vector content)
    : msg_({ProtocolVersion, count(type), std::move(content)}) {
  }

  Message(data msg) : msg_(std::move(caf::get<vector>(msg))) {
  }

  vector msg_;
};

/// A Bro event.
class Event : public Message {
  public:
  Event(std::string name, vector args)
    : Message(Message::Type::Event, {std::move(name), std::move(args)}) {}

  Event(data msg) : Message(std::move(msg)) {}

  const std::string& name() const {
    return caf::get<std::string>(caf::get<vector>(msg_[2])[0]);
  }

  std::string& name() {
    return caf::get<std::string>(caf::get<vector>(msg_[2])[0]);
  }

  const vector& args() const {
    return caf::get<vector>(caf::get<vector>(msg_[2])[1]);
  }

  vector& args() {
    return caf::get<vector>(caf::get<vector>(msg_[2])[1]);
  }
};

/// A batch of other messages.
class Batch : public Message {
  public:
  Batch(vector msgs)
    : Message(Message::Type::Batch, std::move(msgs)) {}

  Batch(data msg) : Message(std::move(msg)) {}

  const vector& batch() const {
    return caf::get<vector>(msg_[2]);
  }

  vector& batch() {
    return caf::get<vector>(msg_[2]);
  }
};

/// A Bro log-create message. Note that at the moment this should be used
/// only by Bro itself as the arguments aren't pulbically defined.
class LogCreate : public Message {
public:
  LogCreate(enum_value stream_id, enum_value writer_id, data writer_info,
            data fields_data)
    : Message(Message::Type::LogCreate,
              {std::move(stream_id), std::move(writer_id),
               std::move(writer_info), std::move(fields_data)}) {
  }

  LogCreate(data msg) : Message(std::move(msg)) {
  }

  const enum_value& stream_id() const {
    return caf::get<enum_value>(caf::get<vector>(msg_[2])[0]);
  }

  enum_value& stream_id() {
    return caf::get<enum_value>(caf::get<vector>(msg_[2])[0]);
  }

  const enum_value& writer_id() const {
    return caf::get<enum_value>(caf::get<vector>(msg_[2])[1]);
  }

  enum_value& writer_id() {
    return caf::get<enum_value>(caf::get<vector>(msg_[2])[1]);
  }

  const data& writer_info() const {
    return caf::get<vector>(msg_[2])[2];
  }

  data& writer_info() {
    return caf::get<vector>(msg_[2])[2];
  }

  const data& fields_data() const {
    return caf::get<vector>(msg_[2])[3];
  }

  data& fields_data() {
    return caf::get<vector>(msg_[2])[3];
  }
};

/// A Bro log-write message. Note that at the moment this should be used only
/// by Bro itself as the arguments aren't publicly defined.
class LogWrite : public Message {
public:
  LogWrite(enum_value stream_id, enum_value writer_id, data path,
           data serial_data)
    : Message(Message::Type::LogWrite,
              {std::move(stream_id), std::move(writer_id),
               std::move(path), std::move(serial_data)}) {
  }

  LogWrite(data msg) : Message(std::move(msg)) {
  }

  const enum_value& stream_id() const {
    return caf::get<enum_value>(caf::get<vector>(msg_[2])[0]);
  }

  enum_value& stream_id() {
    return caf::get<enum_value>(caf::get<vector>(msg_[2])[0]);
  }

  const enum_value& writer_id() const {
    return caf::get<enum_value>(caf::get<vector>(msg_[2])[1]);
  }

  enum_value& writer_id() {
    return caf::get<enum_value>(caf::get<vector>(msg_[2])[1]);
  }

  const data& path() const {
    return caf::get<vector>(msg_[2])[2];
  }

  data& path() {
    return caf::get<vector>(msg_[2])[2];
  };

  const data& serial_data() const {
    return caf::get<vector>(msg_[2])[3];
  }

  data& serial_data() {
    return caf::get<vector>(msg_[2])[3];
  }
};

class IdentifierUpdate : public Message {
public:
  IdentifierUpdate(std::string id_name, data id_value)
    : Message(Message::Type::IdentifierUpdate, {std::move(id_name),
    		                                    std::move(id_value)}) {
  }

  IdentifierUpdate(data msg) : Message(std::move(msg)) {
  }

  const std::string& id_name() const {
    return caf::get<std::string>(caf::get<vector>(msg_[2])[0]);
  }

  std::string& id_name() {
    return caf::get<std::string>(caf::get<vector>(msg_[2])[0]);
  }

  const data& id_value() const {
    return caf::get<vector>(msg_[2])[1];
  }

  data& id_value() {
    return caf::get<vector>(msg_[2])[1];
  }
};

} // namespace broker
} // namespace bro

#endif // BROKER_BRO_HH
