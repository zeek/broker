#pragma once

#include <type_traits>

#include <caf/flow/observer.hpp>

#include "broker/detail/type_traits.hh"
#include "broker/error.hh"
#include "broker/message.hh"

namespace broker::detail {

template <class F>
struct sink_driver_init_trait {
  static_assert(always_false_v<F>, "Init must have signature 'void (State&)'");
};

template <class State>
struct sink_driver_init_trait<void(State&)> {
  static_assert(!std::is_const_v<State>);
  using state_type = State;
};

template <class F>
struct sink_driver_on_next_trait {
  static_assert(always_false_v<F>,
                "OnNext must have signature 'void (State&, data_message)'");
};

template <class State>
struct sink_driver_on_next_trait<void(State&, data_message)> {
  static_assert(!std::is_const_v<State>);
  using state_type = State;
};

template <class State>
struct sink_driver_on_next_trait<void(State&, const data_message&)> {
  static_assert(!std::is_const_v<State>);
  using state_type = State;
};

template <class F>
struct sink_driver_cleanup_trait {
  static_assert(always_false_v<F>,
                "Cleanup must have signature "
                "'void (State&, const error&)' or 'void (State&)'");
};

template <class State>
struct sink_driver_cleanup_trait<void(State&, const error&)> {
  using state_type = std::remove_const_t<State>;

  template <class F>
  static void apply(F& fn, state_type& st) {
    error dummy;
    fn(st, dummy);
  }

  template <class F>
  static void apply(F& fn, state_type& st, const error& err) {
    fn(st, err);
  }
};

template <class State>
struct sink_driver_cleanup_trait<void(State&)> {
  using state_type = std::remove_const_t<State>;

  template <class F>
  static void apply(F& fn, state_type& st) {
    fn(st);
  }

  template <class F>
  static void apply(F& fn, state_type& st, const error&) {
    fn(st);
  }
};

class sink_driver : public caf::flow::observer<data_message>::impl {
public:
  virtual void init() = 0;
};

using sink_driver_ptr = caf::intrusive_ptr<sink_driver>;

template <class Init, class OnNext, class Cleanup>
class sink_driver_impl : public sink_driver {
public:
  using init_trait = sink_driver_init_trait<signature_of_t<Init>>;

  using on_next_trait = sink_driver_on_next_trait<signature_of_t<OnNext>>;

  using cleanup_trait = sink_driver_cleanup_trait<signature_of_t<Cleanup>>;

  using state_type = typename init_trait::state_type;

  static_assert(are_same_v<state_type, typename on_next_trait::state_type,
                           typename cleanup_trait::state_type>);

  sink_driver_impl(Init init_fn, OnNext on_next_fn, Cleanup cleanup_fn)
    : state_(),
      on_next_(std::move(on_next_fn)),
      cleanup_(std::move(cleanup_fn)) {
    new (&init_) Init(std::move(init_fn));
  }

  ~sink_driver_impl() {
    if (!initialized_)
      after_init();
  }

  void init() override {
    if (!initialized_) {
      init_(state_);
      initialized_ = true;
      after_init();
    }
  }

  void on_next(caf::span<const input_type> items) override {
    if (!completed_) {
      for (const auto& item : items)
        on_next_(state_, item);
      sub_.request(items.size());
    }
  }

  void on_error(const caf::error& what) override {
    if (!completed_) {
      cleanup_trait::apply(cleanup_, state_, what);
      sub_ = nullptr;
      completed_ = true;
    }
  }

  void on_complete() override {
    if (!completed_) {
      cleanup_trait::apply(cleanup_, state_);
      sub_ = nullptr;
      completed_ = true;
    }
  }

  void on_attach(caf::flow::subscription sub) override {
    if (!completed_ && !sub_) {
      sub_ = std::move(sub);
      sub_.request(caf::defaults::flow::buffer_size);
    } else {
      sub.cancel();
    }
  }

  void dispose() override {
    if (!completed_) {
      cleanup_trait::apply(cleanup_, state_);
      if (sub_) {
        sub_.cancel();
        sub_ = nullptr;
      }
      completed_ = true;
    }
  }

  bool disposed() const noexcept override {
    return completed_;
  }

private:
  void after_init() {
    init_.~Init();
  }

  state_type state_;
  union {
    Init init_;
  };
  OnNext on_next_;
  Cleanup cleanup_;
  caf::flow::subscription sub_;

  bool initialized_ = false;
  bool completed_ = false;
};

template <class Init, class OnNext, class Cleanup>
sink_driver_ptr make_sink_driver(Init init, OnNext on_next, Cleanup cleanup) {
  using impl_type = sink_driver_impl<Init, OnNext, Cleanup>;
  return caf::make_counted<impl_type>(std::move(init), std::move(on_next),
                                      std::move(cleanup));
}

} // namespace broker::detail
