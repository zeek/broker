#pragma once

#include <caf/flow/fwd.hpp>
#include <caf/make_counted.hpp>
#include <caf/ref_counted.hpp>

#include "broker/fwd.hh"

namespace broker::detail {

class flow_controller_callback : public caf::ref_counted {
public:
  ~flow_controller_callback() override;

  virtual void operator()(flow_controller*) = 0;
};

template <class F>
flow_controller_callback_ptr make_flow_controller_callback(F f) {
  struct impl : flow_controller_callback {
    F fn;

    explicit impl(F fn) : fn(std::move(fn)) {
      // nop
    }

    void operator()(flow_controller* ptr) override {
      fn(ptr);
    }
  };
  return caf::make_counted<impl>(std::move(f));
}

} // namespace broker::detail
