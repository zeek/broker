#include "broker/filter_type.hh"

#include <algorithm>

namespace broker {

namespace {

enum class extend_mode { nop, append, truncate };

extend_mode mode(filter_type& f, const topic& x) {
  for (auto& t : f) {
    if (t == x || t.prefix_of(x)) {
      // Filter already contains x or a less specific subscription.
      return extend_mode::nop;
    }
    if (x.prefix_of(t)) {
      // New topic is less specific than existing entries.
      return extend_mode::truncate;
    }
  }
  return extend_mode::append;
}

} // namespace

bool filter_extend(filter_type& f, const topic& x) {
  switch (mode(f, x)) {
    case extend_mode::append: {
      f.emplace_back(x);
      std::sort(f.begin(), f.end());
      return true;
    }
    case extend_mode::truncate: {
      auto predicate = [&](const topic& y) { return x.prefix_of(y); };
      f.erase(std::remove_if(f.begin(), f.end(), predicate), f.end());
      f.emplace_back(x);
      std::sort(f.begin(), f.end());
      return true;
    }
    default:
      return false;
  }
}

bool filter_extend(filter_type& f, const filter_type& other) {
  size_t count = 0;
  for (auto& x : other)
    count += static_cast<size_t>(filter_extend(f, x));
  return count > 0;
}

} // namespace broker
