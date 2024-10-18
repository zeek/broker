#include "broker/overflow_policy.hh"

#include <cstddef>
#include <string_view>

namespace broker {

void convert(overflow_policy src, std::string& dst) {
  switch (src) {
    case overflow_policy::disconnect:
      dst = "disconnect";
      return;
    case overflow_policy::drop_newest:
      dst = "drop_newest";
      return;
    case overflow_policy::drop_oldest:
      dst = "drop_oldest";
      return;
  }
  dst = "invalid";
}

bool convert(const std::string& src, overflow_policy& dst) {
  std::string_view values[] = {"disconnect", "drop_newest", "drop_oldest"};
  for (size_t index = 0; index < std::size(values); ++index) {
    if (src == values[index]) {
      dst = static_cast<overflow_policy>(index);
      return true;
    }
  }
  return false;
}

} // namespace broker
