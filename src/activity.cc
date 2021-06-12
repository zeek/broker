#include "broker/activity.hh"

#include <future>

#include <caf/send.hpp>

namespace broker {

activity::activity(caf::actor hdl) : hdl_(std::move(hdl)) {
  // nop
}

void activity::cancel() {
  caf::anon_send_exit(hdl_, caf::exit_reason::user_shutdown);
}

void activity::wait() {
  if (hdl_) {
    auto prom = std::make_shared<std::promise<void>>();
    auto fut = prom->get_future();
    hdl_->attach_functor([pptr{std::move(prom)}] { pptr->set_value(); });
    fut.wait();
  }
}

} // namespace broker
