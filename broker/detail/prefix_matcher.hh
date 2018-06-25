#ifndef BROKER_DETAIL_PREFIX_MATCHER_HH
#define BROKER_DETAIL_PREFIX_MATCHER_HH

#include <utility>
#include <vector>

#include <caf/message.hpp>

#include "broker/topic.hh"

namespace broker {
namespace detail {

struct prefix_matcher {
  using filter_type = std::vector<topic>;

  bool operator()(const filter_type& filter, const topic& t) const;

  template <class T>
  bool operator()(const filter_type& filter,
                  const std::pair<topic, T>& x) const {
    return (*this)(filter, x.first);
  }

  bool operator()(const filter_type& filter, const caf::message& msg) const {
    return msg.match_element<topic>(0) && (*this)(filter, msg.get_as<topic>(0));
  }
};


} // namespace detail
} // namespace broker

#endif // BROKER_DETAIL_PREFIX_MATCHER_HH

