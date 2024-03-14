#include "broker/internal/meta_command_writer.hh"

#include <caf/binary_serializer.hpp>

#include "broker/error.hh"
#include "broker/internal_command.hh"

namespace broker::internal {

meta_command_writer::meta_command_writer(caf::binary_serializer& sink)
  : writer_(sink) {
  // nop
}

caf::error meta_command_writer::operator()(const internal_command& x) {
  return std::visit(*this, x.content);
}

caf::error meta_command_writer::operator()(const none& x) {
  return apply_tag(internal_command_uint_tag<none>());
}

caf::error meta_command_writer::operator()(const put_command& x) {
  BROKER_TRY(apply_tag(internal_command_uint_tag<put_command>()),
             writer_(x.key), writer_(x.value));
  return caf::none;
}

caf::error meta_command_writer::operator()(const put_unique_command& x) {
  BROKER_TRY(apply_tag(internal_command_uint_tag<put_unique_command>()),
             writer_(x.key), writer_(x.value));
  return caf::none;
}

caf::error meta_command_writer::operator()(const erase_command& x) {
  BROKER_TRY(apply_tag(internal_command_uint_tag<erase_command>()),
             writer_(x.key));
  return caf::none;
}

caf::error meta_command_writer::operator()(const expire_command& x) {
  BROKER_TRY(apply_tag(internal_command_uint_tag<expire_command>()),
             writer_(x.key));
  return caf::none;
}

caf::error meta_command_writer::operator()(const add_command& x) {
  static_assert(std::is_same_v<uint8_t, std::underlying_type_t<data::type>>);
  BROKER_TRY(apply_tag(internal_command_uint_tag<add_command>()),
             writer_(x.key), writer_(x.value),
             apply_tag(static_cast<uint8_t>(x.init_type)));
  return caf::none;
}

caf::error meta_command_writer::operator()(const subtract_command& x) {
  BROKER_TRY(apply_tag(internal_command_uint_tag<subtract_command>()),
             writer_(x.key), writer_(x.value));
  return caf::none;
}

caf::error meta_command_writer::operator()(const snapshot_command& x) {
  return apply_tag(internal_command_uint_tag<snapshot_command>());
}

caf::error meta_command_writer::operator()(const snapshot_sync_command& x) {
  return apply_tag(internal_command_uint_tag<snapshot_sync_command>());
}

caf::error meta_command_writer::operator()(const set_command& x) {
  BROKER_TRY(apply_tag(internal_command_uint_tag<set_command>()),
             writer_.apply_container(x.state));
  return caf::none;
}

caf::error meta_command_writer::operator()(const clear_command& x) {
  return apply_tag(internal_command_uint_tag<clear_command>());
}

caf::error meta_command_writer::apply_tag(uint8_t tag) {
  auto& sink = writer_.sink();
  if (sink.value(tag))
    return {};
  else
    return sink.get_error();
}

} // namespace broker::internal
