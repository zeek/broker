#ifndef BROKER_MESSAGE_HH
#define BROKER_MESSAGE_HH

#include <caf/make_message.hpp>
#include <caf/message.hpp>

namespace broker {

/// A container for data.
using caf::message;

/// Constructs a message.
using caf::make_message;

} // namespace broker

#endif // BROKER_MESSAGE_HH
