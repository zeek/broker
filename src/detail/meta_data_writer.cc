#include "broker/detail/meta_data_writer.hh"

#include <caf/binary_serializer.hpp>

namespace broker {
namespace detail {

meta_data_writer::meta_data_writer(caf::binary_serializer& sink) : sink_(sink) {
  // nop
}

caf::error meta_data_writer::apply(data::type tag) {
  return sink_(tag);
}

caf::error meta_data_writer::apply(data::type tag, size_t container_size){
  return sink_(tag, static_cast<uint32_t>(container_size));
}

} // namespace detail
} // namespace broker
