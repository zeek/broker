#ifndef BROKER_MESSAGE_HH
#define BROKER_MESSAGE_HH

#include <broker/data.hh>

namespace broker {

/**
 * A message containing a sequence of items.  The meaning/usage of these is
 * left entirely up to the application to decide.
 */
typedef broker::vector message;

} // namespace broker

#endif // BROKER_MESSAGE_HH
