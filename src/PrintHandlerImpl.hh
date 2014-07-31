#ifndef BROKER_PRINTHANDLERIMPL_HH
#define BROKER_PRINTHANDLERIMPL_HH

#include "broker/PrintHandler.hh"

#include <caf/actor.hpp>

namespace broker {

class PrintHandler::Impl {
public:

	std::string topic;
	caf::actor subscriber;
};

} // namespace broker

#endif // BROKER_PRINTHANDLERIMPL_HH
