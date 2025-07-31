#include "broker/none.hh"

#include <string>

namespace broker {

void convert(const none&, std::string& str) {
  str = "nil";
}
} // namespace broker
