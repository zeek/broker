#ifndef PRINTHANDLERIMPL_HH
#define PRINTHANDLERIMPL_HH

#include "broker/PrintHandler.hh"

namespace broker {

class PrintHandler::Impl {
public:

	std::string topic;
	PrintHandler::Callback cb;
	void* cookie;
};

} // namespace broker

#endif // PRINTHANDLERIMPL_HH
