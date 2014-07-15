#ifndef BROKER_BROKER_HH
#define BROKER_BROKER_HH

#include "broker/broker.h"

namespace broker {

/**
 * Initialize the broker library.  This should be called once before using
 * anything else that's provided by the library.
 * @param flags tune behavior of the library.  No flags exist yet.
 * @return 0 if library is initialized, else an error code that can
 *         be supplied to broker::strerror().
 */
int init(int flags = 0);

/**
 * Shutdown the broker library.  No functionality provided by the library
 * is guaranteed to work after the call.
 */
void done();

/**
 * @return a textual representation of a broker error code.
 */
const char* strerror(int errno);

} // namespace broker

#endif // BROKER_BROKER_HH
