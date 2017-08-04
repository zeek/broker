#include "broker/detail/prefix_matcher.hh"

namespace broker {
namespace detail {

bool prefix_matcher::operator()(const filter_type& filter,
                                const topic& t) const {
  for (auto& prefix : filter)
    if (prefix.prefix_of(t))
      return true;
  return false;
}

} // namespace detail
} // namespace broker 
