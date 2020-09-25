#include "broker/alm/multipath.hh"

namespace broker::alm {

void stringify(std::string& buf, const alm::multipath<std::string>& x) {
  buf += '(';
  buf += x.id();
  auto nodes = x.nodes();
  if (!nodes.empty()) {
    buf += ", [";
    auto i = nodes.begin();
    stringify(buf, *i);
    while (++i != nodes.end()) {
      buf += ", ";
      stringify(buf, *i);
    }
    buf += ']';
  }
  buf += ')';
}

std::string to_string(const alm::multipath<std::string>& x) {
  std::string result;
  stringify(result, x);
  return result;
}

} // namespace broker::alm
