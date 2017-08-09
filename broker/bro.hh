#ifndef BROKER_BRO_HH
#define BROKER_BRO_HH

#include "broker/atoms.hh"
#include "broker/data.hh"

namespace broker {
namespace bro {

/// Generic Bro-level message.
class Message {
public:
  enum Type {
    Event = 1,
    LogCreate = 2,
    LogWrite = 3,
  };

  Message(data msg);

  Type type() const {
    return type_;
  }

protected:
  Message(Type type);

  count version_;
  Type type_;
  vector content_;
};

/// A Bro message of a specific type, with the logic to constructy and parse
/// the raw Broker messages.
template <Message::Type ty, typename Args_>
class SpecificMessage : public Message {
public:
  typedef Args_ Args;

  SpecificMessage(Args args) : Message(ty), args_(std::move(args)) {
  }
  SpecificMessage(data msg) : Message(msg) {
    init();
  }
  SpecificMessage(Message&& msg) : Message(msg) {
    init();
  }

  const Args& args() const {
    return args_;
  }

  data as_data() const;

  operator data() const {
    return as_data();
  }

protected:
  void init();

  Args args_;

  Message::Type type() const {
    return type_;
  }
};

struct ArgsEvent {
  std::string name;
  vector args;
};

using EventBase = SpecificMessage<Message::Type::Event, ArgsEvent>;

/// A Bro event.
class Event : public EventBase {
public:
  using EventBase::EventBase;
  Event(std::string name, vector args)
    : EventBase(ArgsEvent{std::move(name), std::move(args)}) {
  }

  const std::string& name() const {
    return args_.name;
  }
  const vector& args() const {
    return args_.args;
  }
};

struct ArgsCreate {
  enum_value stream_id;
  enum_value writer_id;
  data writer_info;
  data fields_data;
};

/// A Bro log-create message. Note that at the moment this should be used
/// only by Bro itself as the arguments aren't pulbically defined.
using LogCreate = SpecificMessage<Message::Type::LogCreate, ArgsCreate>;

struct ArgsWrite {
  enum_value stream_id;
  enum_value writer_id;
  data path;
  data vals_data;
};

/// A Bro log-write message. Note that at the moment this should be used only
/// by Bro itself as the arguments aren't pulbically defined.
using LogWrite = SpecificMessage<Message::Type::LogWrite, ArgsWrite>;

} // namespace broker
} // namespace bro

#endif // BROKER_BRO_HH
