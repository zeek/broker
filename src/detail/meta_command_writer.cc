#include "broker/detail/meta_command_writer.hh"

#include <caf/binary_serializer.hpp>
#include <caf/variant.hpp>

#include "broker/error.hh"
#include "broker/internal_command.hh"

namespace broker::detail {

meta_command_writer::meta_command_writer(caf::binary_serializer& sink)
  : writer_(sink) {
  // nop
}

caf::error meta_command_writer::operator()(const internal_command& x) {
  if (auto err = apply_tag(static_cast<uint8_t>(x.content.index())))
    return err;
  auto f = [this](auto& content) {
    // The inspect overloads require mutable references. However, our writer_
    // never modifies the content, so this const_cast is ugly but safe.
    using type = std::decay_t<decltype(content)>;
    return inspect(writer_, const_cast<type&>(content));
  };
  return caf::visit(f, x.content);
}

caf::error meta_command_writer::apply_tag(uint8_t tag) {
  auto& sink = writer_.sink();
  return sink(tag);
}

} // namespace broker::detail
