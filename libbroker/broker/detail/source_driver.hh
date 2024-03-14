#pragma once

#include "broker/detail/type_traits.hh"
#include "broker/error.hh"
#include "broker/message.hh"

#include <deque>

namespace broker::detail {

// -- trait classes to inspect user-defined callbacks --------------------------

template <class F>
struct source_driver_init_trait {
  static_assert(always_false_v<F>,
                "Init must have signature 'void ()' or 'void (State&)'");
};

template <>
struct source_driver_init_trait<void()> {
  using state_type = void;
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
                "'void (std::deque<data_message>&, size_t)' or "
                "'void (State&, std::deque<data_message>&, size_t)'");
};

template <>
struct source_driver_pull_trait<void(std::deque<data_message>&, size_t)> {
  using state_type = void;
};

template <class State>
struct source_driver_pull_trait<void(State&, std::deque<data_message>&,
                                     size_t)> {
  static_assert(!std::is_const_v<State>);
  using state_type = State;
};

template <class F>
struct source_driver_at_end_trait {
  static_assert(always_false_v<F>,
                "AtEnd must have signature 'bool ()' or 'bool (const State&)'");
};

template <>
struct source_driver_at_end_trait<bool()> {
  using state_type = void;
};

template <class State>
struct source_driver_at_end_trait<bool(const State&)> {
  using state_type = State;
};

// -- generic driver interface -------------------------------------------------

class source_driver {
public:
  virtual ~source_driver();

  virtual void init() = 0;

  virtual void pull(std::deque<data_message>&, size_t) = 0;

  virtual bool at_end() = 0;
};

using source_driver_ptr = std::shared_ptr<source_driver>;

// -- default implementation ---------------------------------------------------

template <class State, class Init, class Pull, class AtEnd>
class source_driver_impl : public source_driver {
public:
  using init_trait = source_driver_init_trait<signature_of_t<Init>>;

  using pull_trait = source_driver_pull_trait<signature_of_t<Pull>>;

  using cleanup_trait = source_driver_at_end_trait<signature_of_t<AtEnd>>;

  using state_type = typename init_trait::state_type;

  static_assert(are_same_v<state_type, typename pull_trait::state_type,
                           typename cleanup_trait::state_type>);

  source_driver_impl(Init init_fn, Pull pull_fn, AtEnd pred)
    : state_(), pull_(std::move(pull_fn)), at_end_(std::move(pred)) {
    new (&init_) Init(std::move(init_fn));
  }

  ~source_driver_impl() override {
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

  State state_;
  union {
    Init init_;
  };
  Pull pull_;
  AtEnd at_end_;
  bool initialized_ = false;
};

template <class Init, class Pull, class AtEnd>
class source_driver_impl<void, Init, Pull, AtEnd> : public source_driver {
public:
  using init_trait = source_driver_init_trait<signature_of_t<Init>>;

  using pull_trait = source_driver_pull_trait<signature_of_t<Pull>>;

  using cleanup_trait = source_driver_at_end_trait<signature_of_t<AtEnd>>;

  source_driver_impl(Init init_fn, Pull pull_fn, AtEnd pred)
    : pull_(std::move(pull_fn)), at_end_(std::move(pred)) {
    new (&init_) Init(std::move(init_fn));
  }

  ~source_driver_impl() override {
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

  void pull(std::deque<data_message>& buf, size_t hint) override {
    pull_(buf, hint);
  }

  bool at_end() override {
    return at_end_();
  }

private:
  void after_init() {
    init_.~Init();
  }

  union {
    Init init_;
  };
  Pull pull_;
  AtEnd at_end_;
  bool initialized_ = false;
};

// -- oracle for checking assertions and selecting the proper implementation ---

template <class Init, class Pull, class AtEnd>
struct source_driver_impl_oracle {
  using init_trait = source_driver_init_trait<signature_of_t<Init>>;

  using pull_trait = source_driver_pull_trait<signature_of_t<Pull>>;

  using at_end_trait = source_driver_at_end_trait<signature_of_t<AtEnd>>;

  using init_state = typename init_trait::state_type;

  using pull_state = typename pull_trait::state_type;

  using at_end_state = typename at_end_trait::state_type;

  static_assert(are_same_v<init_state, pull_state, at_end_state>,
                "Init, Pull, and AtEnd have different state types.");

  using type = source_driver_impl<init_state, Init, Pull, AtEnd>;
};

template <class Init, class Pull, class AtEnd>
using source_driver_impl_t =
  typename source_driver_impl_oracle<Init, Pull, AtEnd>::type;

template <class Init, class Pull, class AtEnd>
auto make_source_driver(Init init, Pull pull, AtEnd at_end) {
  using impl_type = source_driver_impl_t<Init, Pull, AtEnd>;
  return std::make_shared<impl_type>(std::move(init), std::move(pull),
                                     std::move(at_end));
}

} // namespace broker::detail
