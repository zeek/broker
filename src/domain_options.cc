#include "broker/domain_options.hh"

#include <caf/settings.hpp>

namespace broker {

void domain_options::save(caf::settings& sink) {
  caf::put(sink, "broker.disable-forwarding", disable_forwarding);
}

void domain_options::load(const caf::settings& source) {
  using caf::get_or;
  disable_forwarding = get_or(source, "broker.disable-forwarding", false);
}

} // namespace broker
