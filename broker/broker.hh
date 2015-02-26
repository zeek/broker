#ifndef BROKER_BROKER_HH
#define BROKER_BROKER_HH

#include "broker/broker.h"

namespace broker {

/**
 * @see broker_init().
 */
int init(int flags = 0);

/**
 * @see broker_done().
 */
void done();

/**
 * @see broker_strerror
 */
const char* strerror(int broker_errno);

/**
 * @see broker_strerror_r
 */
void strerror_r(int broker_errno, char* buf, size_t buflen);

} // namespace broker

#endif // BROKER_BROKER_HH
