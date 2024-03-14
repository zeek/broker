#pragma once

#include <caf/disposable.hpp>
#include <caf/flow/op/cold.hpp>
#include <caf/scheduled_actor.hpp>
#include <caf/telemetry/counter.hpp>

#include <functional>

namespace broker::internal {

/// Bundles counters that give insight into how much data flows through a scope.
struct flow_scope_stats {
  int64_t requested = 0;
  int64_t delivered = 0;
};

/// @relates flow_scope_stats
using flow_scope_stats_ptr = std::shared_ptr<flow_scope_stats>;

/// Callback that allows client code to react to the destruction of a flow
/// scope.
using flow_scope_stats_deregister_fn =
  std::function<void(const flow_scope_stats_ptr&)>;

template <class Input>
class flow_scope_sub : public caf::ref_counted,
                       public caf::flow::observer_impl<Input>,
                       public caf::flow::subscription_impl {
public:
  // -- member types -----------------------------------------------------------

  using input_type = Input;

  using output_type = input_type;

  // -- constructors, destructors, and assignment operators --------------------

  flow_scope_sub(caf::flow::coordinator* ctx,
                 caf::flow::observer<output_type> out,
                 flow_scope_stats_ptr stats,
                 flow_scope_stats_deregister_fn deregister_cb)
    : ctx_(ctx),
      out_(std::move(out)),
      stats_(std::move(stats)),
      deregister_cb_(std::move(deregister_cb)) {
    // nop
  }

  ~flow_scope_sub() override {
    if (deregister_cb_) {
      try {
        deregister_cb_(stats_);
      } catch (...) {
        // The callbacks may not throw. However, we can't specify them noexcept
        // because std::function does not support noexcept signatures. Hence,
        // this catch-all to silence tool warnings.
      }
    }
  }

  // -- ref counting -----------------------------------------------------------

  void ref_disposable() const noexcept final {
    this->ref();
  }

  void deref_disposable() const noexcept final {
    this->deref();
  }

  void ref_coordinated() const noexcept final {
    this->ref();
  }

  void deref_coordinated() const noexcept final {
    this->deref();
  }

  friend void intrusive_ptr_add_ref(const flow_scope_sub* ptr) noexcept {
    ptr->ref();
  }

  friend void intrusive_ptr_release(const flow_scope_sub* ptr) noexcept {
    ptr->deref();
  }

  // -- implementation of observer_impl<Input> ---------------------------------

  void on_next(const Input& item) override {
    if (out_) {
      ++stats_->delivered;
      out_.on_next(item);
    }
  }

  void on_complete() override {
    in_ = nullptr;
    if (out_) {
      auto tmp = std::move(out_);
      tmp.on_complete();
    }
  }

  void on_error(const caf::error& what) override {
    in_ = nullptr;
    if (out_) {
      auto tmp = std::move(out_);
      tmp.on_error(what);
    }
  }

  void on_subscribe(caf::flow::subscription in) override {
    if (!in_ && out_) {
      in_ = std::move(in);
      if (pre_subscribe_demand_ > 0) {
        in_.request(pre_subscribe_demand_);
        pre_subscribe_demand_ = 0;
      }
    } else {
      in.dispose();
    }
  }

  // -- implementation of subscription_impl ------------------------------------

  bool disposed() const noexcept override {
    return !in_ && !out_;
  }

  void dispose() override {
    if (out_) {
      ctx_->delay_fn([out = std::move(out_)]() mutable { out.on_complete(); });
    }
    if (in_) {
      in_.dispose();
      in_ = nullptr;
    }
  }

  void request(size_t n) override {
    stats_->requested += static_cast<int64_t>(n);
    if (in_)
      in_.request(n);
    else
      pre_subscribe_demand_ += n;
  }

private:
  caf::flow::coordinator* ctx_;
  caf::flow::subscription in_;
  caf::flow::observer<output_type> out_;
  flow_scope_stats_ptr stats_;
  size_t pre_subscribe_demand_ = 0;
  flow_scope_stats_deregister_fn deregister_cb_;
};

/// Decorates an `observable` for instrumentation.
template <class Input>
class flow_scope : public caf::flow::op::cold<Input> {
public:
  using super = caf::flow::op::cold<Input>;

  using decorated_type = caf::flow::observable<Input>;

  flow_scope(decorated_type decorated, flow_scope_stats_ptr stats)
    : super(decorated.ctx()),
      decorated_(std::move(decorated)),
      stats_(std::move(stats)) {
    // nop
  }

  flow_scope(decorated_type decorated, flow_scope_stats_ptr stats,
             flow_scope_stats_deregister_fn deregister_cb)
    : super(decorated.ctx()),
      decorated_(std::move(decorated)),
      stats_(std::move(stats)),
      deregister_cb_(std::move(deregister_cb)) {
    // nop
  }

  caf::disposable subscribe(caf::flow::observer<Input> out) override {
    if (!stats_) {
      out.on_error(make_error(caf::sec::too_many_observers,
                              "flow_scope may only be subscribed to once"));
      return {};
    }
    using sub_t = flow_scope_sub<Input>;
    auto sub = caf::make_counted<sub_t>(this->ctx(), out, std::move(stats_),
                                        std::move(deregister_cb_));
    out.on_subscribe(caf::flow::subscription{sub});
    decorated_.subscribe(caf::flow::observer<Input>{sub});
    return sub->as_disposable();
  }

private:
  decorated_type decorated_;
  flow_scope_stats_ptr stats_;
  flow_scope_stats_deregister_fn deregister_cb_;
};

/// Utility class for injecting a flow_scope to an `observable` without
/// "breaking the chain".
class add_flow_scope_t {
public:
  explicit add_flow_scope_t(flow_scope_stats_ptr stats)
    : stats_(std::move(stats)) {}

  explicit add_flow_scope_t(flow_scope_stats_ptr stats,
                            flow_scope_stats_deregister_fn deregister_cb)
    : stats_(std::move(stats)), deregister_cb_(std::move(deregister_cb)) {}

  template <class Observable>
  auto operator()(Observable&& input) {
    using obs_t = typename std::decay_t<Observable>;
    using val_t = typename obs_t::output_type;
    using impl_t = flow_scope<val_t>;
    auto obs = std::forward<Observable>(input).as_observable();
    auto ptr = caf::make_counted<impl_t>(std::move(obs), std::move(stats_),
                                         std::move(deregister_cb_));
    return caf::flow::observable<val_t>{ptr};
  }

private:
  flow_scope_stats_ptr stats_;
  flow_scope_stats_deregister_fn deregister_cb_;
};

} // namespace broker::internal
