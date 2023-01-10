#pragma once

#include <caf/disposable.hpp>
#include <caf/flow/op/cold.hpp>
#include <caf/scheduled_actor.hpp>

namespace broker::internal {

/// Decorates an `observable` so that any subscription to it can be canceled by
/// calling `dispose` on the kill-switch.
template <class T>
class killswitch : public caf::flow::op::cold<T>, public caf::disposable_impl {
public:
  using super = caf::flow::op::cold<T>;

  killswitch(caf::flow::observable<T> decorated)
    : super(decorated.ctx()), decorated_(std::move(decorated)) {
    // nop
  }

  caf::disposable subscribe(caf::flow::observer<T> out) override {
    if (disposed_) {
      out.on_error(make_error(caf::sec::disposed));
      return {};
    }
    if (!decorated_) {
      out.on_error(make_error(caf::sec::too_many_observers,
                              "killswitch may only be subscribed to once"));
      return {};
    }
    BROKER_ASSERT(!sub_);
    sub_ = decorated_.subscribe(out);
    decorated_ = nullptr;
    return sub_;
  }

  void dispose() override {
    if (!disposed_) {
      disposed_ = true;
      decorated_ = nullptr;
      sub_.dispose();
    }
  }

  bool disposed() const noexcept override {
    return disposed_;
  }

  void ref_disposable() const noexcept override {
    this->ref();
  }

  void deref_disposable() const noexcept override {
    this->deref();
  }

private:
  bool disposed_ = false;
  caf::flow::observable<T> decorated_;
  caf::disposable sub_;
};

template <class T>
void intrusive_ptr_add_ref(killswitch<T>* ptr) {
  ptr->ref();
}

template <class T>
void intrusive_ptr_release(killswitch<T>* ptr) {
  ptr->deref();
}

/// Utility class for adding a killswitch to an `observable`.
struct add_killswitch_t {
  template <class Observable>
  auto operator()(Observable&& input) const {
    using obs_t = typename std::decay_t<Observable>;
    using val_t = typename obs_t::output_type;
    using impl_t = killswitch<val_t>;
    auto obs = std::forward<Observable>(input).as_observable();
    auto ptr = caf::make_counted<impl_t>(std::move(obs));
    return std::pair{caf::flow::observable<val_t>{ptr}, ptr->as_disposable()};
  }
};

/// Utility class for injecting a killswitch to an `observable` without
/// "breaking the chain".
struct inject_killswitch_t {
  caf::disposable* result;

  explicit inject_killswitch_t(caf::disposable* result_ptr)
    : result(result_ptr) {
    // nop
  }

  template <class Observable>
  auto operator()(Observable&& input) const {
    using obs_t = typename std::decay_t<Observable>;
    using val_t = typename obs_t::output_type;
    using impl_t = killswitch<val_t>;
    auto obs = std::forward<Observable>(input).as_observable();
    auto ptr = caf::make_counted<impl_t>(std::move(obs));
    *result = ptr->as_disposable();
    return caf::flow::observable<val_t>{ptr};
  }
};

} // namespace broker::internal
