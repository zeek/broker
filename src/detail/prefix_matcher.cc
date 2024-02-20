#include "broker/detail/prefix_matcher.hh"

namespace broker::detail {

bool prefix_matcher::operator()(const filter_type& filter,
                                std::string_view t) const noexcept {
  for (auto& prefix : filter)
    if (topic::is_prefix(t, prefix.string()))
      return true;
  return false;
}

} // namespace broker::detail
