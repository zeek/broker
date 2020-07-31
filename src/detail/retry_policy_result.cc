#include "broker/detail/retry_policy_result.hh"

namespace broker::detail {

std::string to_string(retry_policy_result x) {
  switch (x) {
    case retry_policy_result::wait:
      return "wait";
      break;
    case retry_policy_result::try_again:
      return "try_again";
      break;
    case retry_policy_result::abort:
      return "abort";
      break;
    default:
      return "???";
  }
}

} // namespace broker::detail
