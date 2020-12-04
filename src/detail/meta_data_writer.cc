#include "broker/detail/meta_data_writer.hh"

#include <caf/binary_serializer.hpp>

namespace broker::detail {

meta_data_writer::meta_data_writer(caf::binary_serializer& sink) : sink_(sink) {
  // nop
}

caf::error meta_data_writer::apply(data::type tag) {
  auto val = static_cast<std::underlying_type_t<data::type>>(tag);
  if (sink_.value(val))
    return {};
  else
    return sink_.get_error();
}

caf::error meta_data_writer::apply(size_t container_size) {
  auto val = static_cast<uint32_t>(container_size);
  if (sink_.value(val))
    return {};
  else
    return sink_.get_error();
}

} // namespace broker::detail
