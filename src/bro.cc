#include "broker/bro.hh"
#include "broker/data.hh"

namespace broker {
namespace bro {

static const count ProtocolVersion = 1;

Message::Message(Type type) : type_(type) {
}

Message::Message(data x) {
  auto v = get<vector>(x);

  if (v.size() != 2)
    throw std::runtime_error("broken Bro message header");

  auto hdr = get<vector>(v[0]);
  content_ = get<vector>(v[1]);

  if (hdr.size() != 2)
    throw std::runtime_error("broken Bro message header");

  version_ = get<count>(hdr[0]);

  if (version_ != ProtocolVersion)
    throw std::runtime_error("unsupported Bro message version");

  switch (get<count>(hdr[1])) {
    case Type::Event:
      type_ = Type::Event;
      break;
    case Type::LogCreate:
      type_ = Type::LogCreate;
      break;
    case Type::LogWrite:
      type_ = Type::LogWrite;
      break;
    case Type::IdentifierUpdate:
      type_ = Type::IdentifierUpdate;
      break;
    default:
      throw std::runtime_error("unsupported Bro message type");
  };
}

template <>
void EventBase::init() {
  if (type_ != Type::Event)
    throw std::runtime_error("not a Bro event");

  if (content_.size() != 2)
    throw std::runtime_error("broken Bro event");

  try {
    args_.name = get<std::string>(content_[0]);
    args_.args = get<vector>(content_[1]);
  } catch (const bad_variant_access& e) {
    throw std::runtime_error("unexpected Bro event arguments");
  }
}

template <>
data EventBase::as_data() const {
  auto hdr = vector{ProtocolVersion, static_cast<count>(Type::Event)};
  auto content = vector{args_.name, args_.args};
  return vector{std::move(hdr), std::move(content)};
}

template <>
void LogCreate::init() {
  if (type_ != Type::LogCreate)
    throw std::runtime_error("not a Bro LogCreate");

  if (content_.size() != 4)
    throw std::runtime_error("broken Bro LogCreate");

  try {
    args_.stream_id = get<enum_value>(content_[0]);
    args_.writer_id = get<enum_value>(content_[1]);
    args_.writer_info = content_[2];
    args_.fields_data = content_[3];
  } catch (const bad_variant_access& e) {
    throw std::runtime_error("unexpected Bro LogCreate arguments");
  }
}

template <>
data LogCreate::as_data() const {
  auto hdr = vector{ProtocolVersion, static_cast<count>(Type::LogCreate)};
  auto args = vector{args_.stream_id, args_.writer_id, args_.writer_info,
                     args_.fields_data};
  return vector{std::move(hdr), std::move(args)};
}

template <>
void LogWrite::init() {
  if (type_ != Type::LogWrite)
    throw std::runtime_error("not a Bro LogWrite");

  if (content_.size() != 4)
    throw std::runtime_error("broken Bro LogWrite");

  try {
    args_.stream_id = get<enum_value>(content_[0]);
    args_.writer_id = get<enum_value>(content_[1]);
    args_.path = content_[2];
    args_.vals_data = content_[3];
  } catch (const bad_variant_access& e) {
    throw std::runtime_error("unexpected Bro LogWrite arguments");
  }
}

template <>
data LogWrite::as_data() const {
  auto hdr = vector{ProtocolVersion, static_cast<count>(Type::LogWrite)};
  auto args
    = vector{args_.stream_id, args_.writer_id, args_.path, args_.vals_data};
  return vector{std::move(hdr), std::move(args)};
}

template <>
void IdentifierUpdate::init() {
  if (type_ != Type::IdentifierUpdate)
    throw std::runtime_error("not a Bro IdentifierUpdate");

  if (content_.size() != 2)
    throw std::runtime_error("broken Bro IdentifierUpdate");

  try {
    args_.id_name = get<std::string>(content_[0]);
    args_.id_value = content_[1];
  } catch (const bad_variant_access& e) {
    throw std::runtime_error("unexpected Bro IdentifierUpdate arguments");
  }
}

template <>
data IdentifierUpdate::as_data() const {
  auto hdr = vector{ProtocolVersion, static_cast<count>(Type::IdentifierUpdate)};
  auto content = vector{args_.id_name, args_.id_value};
  return vector{std::move(hdr), std::move(content)};
}
}
}
