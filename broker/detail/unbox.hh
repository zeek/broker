#ifndef BROKER_DETAIL_UNBOX_HPP
#define BROKER_DETAIL_UNBOX_HPP

#include <caf/result.hpp>

#include "broker/result.hh"

namespace broker {
namespace detail {

template <class T>
caf::result<T> unbox(broker::result<T>&& x) {
  if (x)
    return std::move(*x);
  return make_error(x.status());
}

} // namespace detail
} // namespace broker

#endif
