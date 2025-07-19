#include "broker/enum_value.hh"

namespace broker {

void convert(const enum_value& e, std::string& str) {
  str = e.name;
}

void convert(const enum_value_view& e, std::string& str) {
  str = e.name;
}

} // namespace broker
