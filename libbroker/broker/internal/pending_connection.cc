#include "broker/internal/pending_connection.hh"

// Note: implementations of this interface are in connector.cc.

namespace broker::internal {

pending_connection::~pending_connection() {
  // nop
}

} // namespace broker::internal
