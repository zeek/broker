#include "broker/version.hh"

namespace broker::version {

std::string string() {
  using std::to_string;
  return to_string(version::major) + '.' + to_string(version::minor) + '.'
         + to_string(version::patch) + version::suffix;
}

} // namespace broker::version
