#ifndef PRINTHANDLERIMPL_HH
#define PRINTHANDLERIMPL_HH

#include "broker/PrintHandler.hh"

#include <cppa/cppa.hpp>

namespace broker {

class PrintHandler::Impl {
public:

	std::string topic;
	cppa::actor subscriber;
};

} // namespace broker

#endif // PRINTHANDLERIMPL_HH
