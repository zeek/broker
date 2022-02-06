#pragma once

#include "broker/detail/type_traits.hh"
#include "broker/error.hh"
#include "broker/message.hh"

#include <deque>

namespace broker::detail {

template <class F>
struct source_driver_init_trait {
  static_assert(always_false_v<F>, "Init must have signature 'void (State&)'");
};

template <class State>
struct source_driver_init_trait<void(State&)> {
  static_assert(!std::is_const_v<State>);
  using state_type = State;
};

template <class F>
struct source_driver_pull_trait {
  static_assert(always_false_v<F>,
                "Pull must have signature "
                "'void (State&, std::deque<data_message>&, size_t)'");
};

template <class State>
struct source_driver_pull_trait<void(State&, data_message)> {
  static_assert(!std::is_const_v<State>);
  using state_type = State;
};

template <class State>
struct source_driver_pull_trait<void(State&, std::deque<data_message>&,
                                     size_t)> {
  static_assert(!std::is_const_v<State>);
  using state_type = State;
};

template <class F>
struct source_driver_cleanup_trait {
  static_assert(always_false_v<F>,
                "AtEnd must have signature 'bool (const State&)'");
};

template <class State>
struct source_driver_cleanup_trait<bool(const State&)> {
  using state_type = State;
};

class source_driver {
public:
  virtual ~source_driver();

  virtual void init() = 0;

  virtual void pull(std::deque<data_message>&, size_t) = 0;

  virtual bool at_end() = 0;
};

using source_driver_ptr = std::shared_ptr<source_driver>;

template <class Init, class Pull, class AtEnd>
class source_driver_impl : public source_driver {
public:
  using init_trait = source_driver_init_trait<signature_of_t<Init>>;

  using pull_trait = source_driver_pull_trait<signature_of_t<Pull>>;

  using cleanup_trait = source_driver_cleanup_trait<signature_of_t<AtEnd>>;

  using state_type = typename init_trait::state_type;

  static_assert(are_same_v<state_type, typename pull_trait::state_type,
                           typename cleanup_trait::state_type>);

  source_driver_impl(Init init_fn, Pull pull_fn, AtEnd pred)
    : state_(), pull_(std::move(pull_fn)), at_end_(std::move(pred)) {
    new (&init_) Init(std::move(init_fn));
  }

  ~source_driver_impl() {
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

  void pull(std::deque<data_message>& buf, size_t hint) override {
    pull_(state_, buf, hint);
  }

  bool at_end() override {
    return at_end_(state_);
  }

private:
  void after_init() {
    init_.~Init();
  }

  state_type state_;
  union {
    Init init_;
  };
  Pull pull_;
  AtEnd at_end_;
  bool initialized_ = false;
};

} // namespace broker::detail
