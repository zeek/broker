#include "broker/variant_list.hh"

#include "broker/format/txt.hh"

namespace broker {

std::ostream& operator<<(std::ostream& out, const variant_list& what) {
  format::txt::v1::encode(what.raw(), std::ostream_iterator<char>(out));
  return out;
}

} // namespace broker
