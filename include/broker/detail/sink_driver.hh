#pragma once

#include "broker/detail/type_traits.hh"
#include "broker/error.hh"
#include "broker/message.hh"

#include <memory>

namespace broker::detail {

// -- trait classes to inspect user-defined callbacks --------------------------

template <class F>
struct sink_driver_init_trait {
  static_assert(always_false_v<F>,
                "Init must have signature 'void ()' or 'void (State&)'");
};

template <>
struct sink_driver_init_trait<void()> {
  using state_type = void;
};

template <class State>
struct sink_driver_init_trait<void(State&)> {
  static_assert(!std::is_const_v<State>);
  using state_type = State;
};

template <class F>
struct sink_driver_on_next_trait {
  static_assert(always_false_v<F>,
                "OnNext must have signature 'void (data_message)' "
                "or 'void (State&, data_message)'");
};

template <>
struct sink_driver_on_next_trait<void(data_message)> {
  using state_type = void;
};

template <>
struct sink_driver_on_next_trait<void(const data_message&)> {
  using state_type = void;
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
                "Cleanup must have signature 'void (const error&)' "
                "or 'void (State&, const error&)'");
};

template <>
struct sink_driver_cleanup_trait<void(const error&)> {
  using state_type = void;
};

template <class State>
struct sink_driver_cleanup_trait<void(State&, const error&)> {
  using state_type = std::remove_const_t<State>;
};

// -- generic driver interface -------------------------------------------------

class sink_driver {
public:
  virtual ~sink_driver();

  virtual void init() = 0;

  virtual void on_next(const data_message& msg) = 0;

  virtual void on_cleanup(const error& what) = 0;
};

// -- default implementation ---------------------------------------------------

using sink_driver_ptr = std::shared_ptr<sink_driver>;

template <class State, class Init, class OnNext, class Cleanup>
class sink_driver_impl : public sink_driver {
public:
  using init_trait = sink_driver_init_trait<signature_of_t<Init>>;

  using on_next_trait = sink_driver_on_next_trait<signature_of_t<OnNext>>;

  using cleanup_trait = sink_driver_cleanup_trait<signature_of_t<Cleanup>>;

  template <class InitFn, class OnNextFn, class CleanupFn>
  sink_driver_impl(InitFn&& init_fn, OnNextFn&& on_next_fn,
                   CleanupFn&& cleanup_fn)
    : state_(),
      on_next_(std::forward<OnNextFn>(on_next_fn)),
      cleanup_(std::forward<CleanupFn>(cleanup_fn)) {
    new (&init_) Init(std::forward<InitFn>(init_fn));
  }

  ~sink_driver_impl() override {
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

  void on_next(const data_message& msg) override {
    if (!completed_)
      on_next_(state_, msg);
  }

  void on_cleanup(const error& what) override {
    if (!completed_) {
      cleanup_(state_, what);
      completed_ = true;
    }
  }

private:
  void after_init() {
    init_.~Init();
  }

  State state_;
  union {
    Init init_;
  };
  OnNext on_next_;
  Cleanup cleanup_;

  bool initialized_ = false;
  bool completed_ = false;
};

template <class Init, class OnNext, class Cleanup>
class sink_driver_impl<void, Init, OnNext, Cleanup> : public sink_driver {
public:
  using init_trait = sink_driver_init_trait<signature_of_t<Init>>;

  using on_next_trait = sink_driver_on_next_trait<signature_of_t<OnNext>>;

  using cleanup_trait = sink_driver_cleanup_trait<signature_of_t<Cleanup>>;

  sink_driver_impl(Init init_fn, OnNext on_next_fn, Cleanup cleanup_fn)
    : on_next_(std::move(on_next_fn)), cleanup_(std::move(cleanup_fn)) {
    new (&init_) Init(std::move(init_fn));
  }

  ~sink_driver_impl() override {
    if (!initialized_)
      after_init();
  }

  void init() override {
    if (!initialized_) {
      init_();
      initialized_ = true;
      after_init();
    }
  }

  void on_next(const data_message& msg) override {
    if (!completed_)
      on_next_(msg);
  }

  void on_cleanup(const error& what) override {
    if (!completed_) {
      cleanup_(what);
      completed_ = true;
    }
  }

private:
  void after_init() {
    init_.~Init();
  }

  union {
    Init init_;
  };
  OnNext on_next_;
  Cleanup cleanup_;

  bool initialized_ = false;
  bool completed_ = false;
};

// -- oracle for checking assertions and selecting the proper implementation ---

template <class Init, class OnNext, class Cleanup>
struct sink_driver_impl_oracle {
  using init_trait = sink_driver_init_trait<signature_of_t<Init>>;

  using on_next_trait = sink_driver_on_next_trait<signature_of_t<OnNext>>;

  using cleanup_trait = sink_driver_cleanup_trait<signature_of_t<Cleanup>>;

  using init_state = typename init_trait::state_type;

  using on_next_state = typename on_next_trait::state_type;

  using cleanup_state = typename cleanup_trait::state_type;

  static_assert(are_same_v<init_state, on_next_state, cleanup_state>,
                "Init, OnNext, and Cleanup have different state types.");

  using type = sink_driver_impl<init_state, Init, OnNext, Cleanup>;
};

template <class Init, class OnNext, class Cleanup>
using sink_driver_impl_t =
  typename sink_driver_impl_oracle<Init, OnNext, Cleanup>::type;

template <class Init, class OnNext, class Cleanup>
auto make_sink_driver(Init init, OnNext on_next, Cleanup cleanup) {
  using impl_type = sink_driver_impl_t<Init, OnNext, Cleanup>;
  return std::make_shared<impl_type>(std::move(init), std::move(on_next),
                                     std::move(cleanup));
}

} // namespace broker::detail
