#include "broker/detail/sink_driver.hh"

namespace broker::detail {

sink_driver::~sink_driver() {
  // nop
}

void sink_driver::ref_disposable() const noexcept {
  ref();
}

void sink_driver::deref_disposable() const noexcept {
  deref();
}

} // namespace broker::detail
