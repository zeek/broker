#ifndef BROKER_FWD_HH
#define BROKER_FWD_HH

#include <cstdint>

namespace broker {

class context;
class endpoint;
class blocking_endpoint;
class nonblocking_endpoint;

struct network_info;

using endpoint_id = uint64_t;

} // namespace broker

#endif // BROKER_FWD_HH
