#ifndef BROKER_BROKER_HH
#define BROKER_BROKER_HH

#include "broker/broker.h"

namespace broker {

/**
 * @see broker_init().
 */
int init(int flags = 0);

const auto done = broker_done;

const auto strerror = broker_strerror;

const auto strerror_r = broker_strerror_r;

} // namespace broker

#endif // BROKER_BROKER_HH
