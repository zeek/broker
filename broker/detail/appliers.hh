#ifndef BROKER_DETAIL_APPLIERS_HH
#define BROKER_DETAIL_APPLIERS_HH

#include "broker/data.hh"
#include "broker/error.hh"
#include "broker/expected.hh"
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
  using result_type = expected<void>;

  template <class T>
  auto operator()(T&) -> disable_if_t<is_additive_group<T>(), result_type> {
    return ec::type_clash;
  }

  template <class T>
  auto operator()(T& c) -> enable_if_t<is_additive_group<T>(), result_type> {
    auto x = value.get<T>();
    if (!x)
      return ec::type_clash;
    c += *x;
    return {};
  }

  result_type operator()(time::point& tp) {
    auto d = value.get<time::duration>();
    if (!d)
      return ec::type_clash;
    tp += *d;
    return {};
  }

  result_type operator()(std::string& str) {
    auto x = value.get<std::string>();
    if (!x)
      return ec::type_clash;
    str += *x;
    return {};
  }

  result_type operator()(vector& v) {
    v.push_back(value);
    return {};
  }

  result_type operator()(set& s) {
    s.insert(value);
    return {};
  }

  result_type operator()(table& t) {
    // Data must come as key-value pair to be valid, which we model as
    // vector of length 2.
    auto v = value.get<vector>();
    if (!v)
      return ec::type_clash;
    if (v->size() != 2)
      return ec::invalid_data;
    t[v->front()] = v->back();
    return {};
  }

  const data& value;
};

struct remover {
  using result_type = expected<void>;

  template <class T>
  auto operator()(T&) -> disable_if_t<is_additive_group<T>(), result_type> {
    return ec::type_clash;
  }

  template <class T>
  auto operator()(T& c) -> enable_if_t<is_additive_group<T>(), result_type> {
    auto x = value.get<T>();
    if (!x)
      return ec::type_clash;
    c -= *x;
    return {};
  }

  result_type operator()(time::point& tp) {
    auto d = value.get<time::duration>();
    if (!d)
      return ec::type_clash;
    tp -= *d;
    return {};
  }

  result_type operator()(vector& v) {
    if (!v.empty())
      v.pop_back();
    return {};
  }

  result_type operator()(set& s) {
    s.erase(value);
    return {};
  }

  result_type operator()(table& t) {
    t.erase(value);
    return {};
  }

  const data& value;
};

struct retriever {
  using result_type = expected<data>;

  template <class T>
  result_type operator()(const T& x) const {
    return x;
  }

  result_type operator()(const vector& v) const {
    auto i = aspect.get<count>();
    if (!i)
      return ec::type_clash;
    if (*i >= v.size())
      return ec::invalid_data;
    return v[*i];
  }

  result_type operator()(const set& s) const {
    return s.count(aspect) == 1;
  }

  result_type operator()(const table& t) const {
    auto i = t.find(aspect);
    if (i == t.end())
      return ec::invalid_data;
    return i->second;
  }

  const data& aspect;
};

} // namespace detail
} // namespace broker

#endif // BROKER_DETAIL_APPLIERS_HH
