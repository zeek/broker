#ifndef BROKER_STORE_MEMORY_BACKEND_IMPL_HH
#define BROKER_STORE_MEMORY_BACKEND_IMPL_HH

#include <unordered_map>

#include "broker/store/memory_backend.hh"

namespace broker {
namespace store {

class memory_backend::impl {
public:
  sequence_num sn;
  std::unordered_map<data, value> datastore;
  std::string last_error;
};

} // namespace store
} // namespace broker

#endif // BROKER_STORE_MEMORY_BACKEND_IMPL_HH
