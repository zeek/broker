#include "broker/backend.hh"

namespace broker {

void convert(const backend& src, std::string& dst) {
  if (src == backend::memory)
    dst = "memory";
  else if (src == backend::sqlite)
    dst = "sqlite";
}

} // namespace broker
