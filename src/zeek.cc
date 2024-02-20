#include "broker/zeek.hh"

namespace broker::zeek {

Message::~Message() {}

void Message::init(Type sub_type, const list_builder& content) {
  data_ = list_builder{}
            .add(ProtocolVersion)
            .add(static_cast<count>(sub_type))
            .add(content)
            .build();
}

Batch::Batch(variant elements) : Message(std::move(elements)) {
  if (!validate_outer_fields(Type::Batch))
    return;
  auto&& items = sub_fields(); // Each field in the content is a message.
  auto tmp = std::make_shared<Content>();
  tmp->reserve(items.size());
  auto append = [&tmp](auto&& msg) {
    if (!msg.valid())
      return false;
    tmp->emplace_back(std::forward<decltype(msg)>(msg));
    return true;
  };
  for (auto&& item : items) {
    switch (Message::type(item)) {
      case Message::Type::Event:
        if (!append(zeek::Event{item}))
          return;
        break;
      case Message::Type::LogCreate:
        if (!append(zeek::LogCreate{item}))
          return;
        break;
      case Message::Type::LogWrite:
        if (!append(zeek::LogWrite{item}))
          return;
        break;
      case Message::Type::IdentifierUpdate:
        if (!append(zeek::IdentifierUpdate{item}))
          return;
        break;
      case Message::Type::Batch:
        if (!append(zeek::Batch{item}))
          return;
        break;
      default:
        return;
    }
  }
  impl_ = std::move(tmp);
}

Batch BatchBuilder::build() {
  auto result = Batch{list_builder{}
                        .add(ProtocolVersion)
                        .add(static_cast<count>(Message::Type::Batch))
                        .add(inner_)
                        .build()};
  inner_.reset();
  return result;
}

} // namespace broker::zeek
