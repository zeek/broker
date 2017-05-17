#include "broker/internal_command.hh"

namespace broker {

internal_command::internal_command(variant_type x) : content(std::move(x)) {
  // nop
}

} // namespace broker
