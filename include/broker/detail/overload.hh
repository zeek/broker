#pragma once

#include <utility>

namespace broker::detail {

template <class... Fs>
struct overload;

template <class F>
struct overload<F> : F {
  using F::operator();
  overload(F f) : F(f) {
    // nop
  }
};

template <class F, class... Fs>
struct overload<F, Fs...> : F, overload<Fs...> {
  using F::operator();
  using overload<Fs...>::operator();
  overload(F f, Fs... fs) : F(f), overload<Fs...>(fs...) {
    // nop
  }
};

template <class... Fs>
overload<Fs...> make_overload(Fs... fs) {
  return {std::move(fs)...};
}

} // namespace broker::detail
