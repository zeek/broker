#include "broker/variant_list.hh"

#include "broker/format/txt.hh"

#include <iterator>

namespace broker {

void convert(const variant_list& value, std::string& out) {
  format::txt::v1::encode(value.raw(), std::back_inserter(out));
}

std::ostream& operator<<(std::ostream& out, const variant_list& what) {
  format::txt::v1::encode(what.raw(), std::ostream_iterator<char>(out));
  return out;
}

} // namespace broker
