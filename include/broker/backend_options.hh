#ifndef BROKER_BACKEND_OPTIONS_HH
#define BROKER_BACKEND_OPTIONS_HH

#include <string>
#include <unordered_map>

#include "broker/data.hh"

namespace broker {

using backend_options = std::unordered_map<std::string, data>;

} // namespace broker

#endif // BROKER_BACKEND_OPTIONS_HH
