#ifndef BROKER_DETAIL_APPLIERS_HH
#define BROKER_DETAIL_APPLIERS_HH

#include "broker/data.hh"
#include "broker/message.hh"
#include "broker/time.hh"

#include "broker/detail/type_traits.hh"

namespace broker {
namespace detail {

template <class T>
constexpr bool is_additive_group() {
  return std::is_same<T, count>::value
    || std::is_same<T, integer>::value
    || std::is_same<T, real>::value
    || std::is_same<T, time::duration>::value;
}

struct adder {
  using result_type = bool;

  template <class T>
  auto operator()(T&) -> disable_if_t<is_additive_group<T>(), result_type> {
    return false;
  }

  template <class T>
  auto operator()(T& c) -> enable_if_t<is_additive_group<T>(), result_type> {
    auto x = value.get<T>();
    if (!x)
      return false;
    c += *x;
    return true;
  }

  result_type operator()(time::point& tp) {
    auto d = value.get<time::duration>();
    if (!d)
      return false;
    tp += *d;
    return true;
  }

  result_type operator()(std::string& str) {
    auto x = value.get<std::string>();
    if (!x)
      return false;
    str += *x;
    return true;
  }

  result_type operator()(vector& v) {
    v.push_back(value);
    return true;
  }

  result_type operator()(set& s) {
    s.insert(value);
    return true;
  }

  result_type operator()(table& t) {
    // Data must come as key-value pair to be valid, which we model as
    // vector of length 2.
    auto v = value.get<vector>();
    if (!v || v->size() != 2)
      return false;
    t[v->front()] = v->back();
    return true;
  }

  const data& value;
};

struct remover {
  using result_type = bool;

  template <class T>
  auto operator()(T&) -> disable_if_t<is_additive_group<T>(), result_type> {
    return false;
  }

  template <class T>
  auto operator()(T& c) -> enable_if_t<is_additive_group<T>(), result_type> {
    auto x = value.get<T>();
    if (!x)
      return false;
    c -= *x;
    return true;
  }

  result_type operator()(time::point& tp) {
    auto d = value.get<time::duration>();
    if (!d)
      return false;
    tp -= *d;
    return true;
  }

  result_type operator()(vector& v) {
    if (!v.empty())
      v.pop_back();
    return true;
  }

  result_type operator()(set& s) {
    s.erase(value);
    return true;
  }

  result_type operator()(table& t) {
    t.erase(value);
    return true;
  }

  const data& value;
};

struct retriever {
  using result_type = data;

  template <class T>
  result_type operator()(const T& x) const {
    return x;
  }

  result_type operator()(const vector& v) const {
    auto i = aspect.get<count>();
    if (!i || *i >= v.size())
      return nil;
    return v[*i];
  }

  result_type operator()(const set& s) const {
    return s.count(aspect) == 1;
  }

  result_type operator()(const table& t) const {
    // Data must come as key-value pair to be valid, which we model as
    // vector of length 2.
    auto i = t.find(aspect);
    if (i == t.end())
      return nil;
    return i->second;
  }

  const data& aspect;
};

} // namespace detail
} // namespace broker

#endif // BROKER_DETAIL_APPLIERS_HH
