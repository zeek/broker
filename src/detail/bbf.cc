#include "broker/detail/bbf.hh"

#include "broker/data_view.hh"

#include <caf/detail/ieee_754.hpp>
#include <caf/detail/network_order.hpp>

namespace broker::detail::bbf {

uint16_t to_network_order(uint16_t value) {
  return caf::detail::to_network_order(value);
}

uint64_t to_network_order(uint64_t value) {
  return caf::detail::to_network_order(value);
}

uint64_t real_to_network_representation(real value) {
  return caf::detail::pack754(value);
}

real real_from_network_representation(uint64_t value) {
  return caf::detail::unpack754(value);
}

} // namespace broker::detail::bbf
