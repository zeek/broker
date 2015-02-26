#ifndef BROKER_TOPIC_HH
#define BROKER_TOPIC_HH

#include <string>

namespace broker {

/**
 * A topic string used for broker's supported communication patterns.
 * Messaging has a pub/sub communication pattern where subscribers advertise
 * interest in all topics that match a given prefix.
 */
typedef std::string topic;

} // namespace broker

#endif // BROKER_TOPIC_HH
