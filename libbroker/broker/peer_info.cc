#include "broker/peer_info.hh"

namespace broker {

void convert(const peer_info& x, std::string& str) {
  str = "peer_info(";
  str += to_string(x.peer);
  str += ", ";
  str += std::to_string(static_cast<int>(x.flags));
  str += ", ";
  str += to_string(x.status);
  str += ")";
}

} // namespace broker
