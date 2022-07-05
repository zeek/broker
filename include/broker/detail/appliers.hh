#pragma once

#include "broker/data.hh"
#include "broker/error.hh"
#include "broker/expected.hh"
#include "broker/status.hh"
#include "broker/time.hh"

#include "broker/detail/type_traits.hh"

namespace broker::detail {

template <class T>
inline constexpr bool is_additive_group_v = std::is_same_v<T, count>      //
                                            || std::is_same_v<T, integer> //
                                            || std::is_same_v<T, real>    //
                                            || std::is_same_v<T, timespan>;

struct adder {
  using result_type = expected<void>;

  template <class T>
  result_type operator()(T& c) {
    if constexpr (is_additive_group_v<T>) {
      if (auto x = get_if<T>(&value)) {
        c += *x;
        return {};
      } else {
        return ec::type_clash;
      }
    } else {
      return ec::type_clash;
    }
  }

  result_type operator()(timestamp& tp) {
    if (auto s = get_if<timespan>(&value)) {
      tp += *s;
      return {};
    } else {
      return ec::type_clash;
    }
  }

  result_type operator()(std::string& str) {
    if (auto x = get_if<std::string>(&value)) {
      str += *x;
      return {};
    } else {
      return ec::type_clash;
    }
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
    auto v = get_if<vector>(&value);
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
  result_type operator()(T& c) {
    if constexpr (is_additive_group_v<T>) {
      if (auto x = get_if<T>(&value)) {
        c -= *x;
        return {};
      } else {
        return ec::type_clash;
      }
    } else {
      return ec::type_clash;
    }
  }

  result_type operator()(timestamp& ts) {
    auto s = get_if<timespan>(&value);
    if (!s)
      return ec::type_clash;
    ts -= *s;
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

  static result_type at_index(const vector& v, count index) {
    if (index < v.size()) {
      return v[index];
    } else {
      return ec::no_such_key;
    }
  }

  result_type operator()(const vector& v) const {
    if (auto x = get_if<count>(&aspect)) {
      return at_index(v, *x);
    } else {
      if (auto y = get_if<integer>(&aspect); y && *y >= 0) {
        return at_index(v, static_cast<count>(*y));
      } else {
        return ec::type_clash;
      }
    }
  }

  result_type operator()(const set& s) const {
    return s.count(aspect) == 1;
  }

  result_type operator()(const table& t) const {
    auto i = t.find(aspect);
    if (i == t.end())
      return ec::no_such_key;
    return i->second;
  }

  const data& aspect;
};

} // namespace broker::detail
