#include "PrintHandlerImpl.hh"

broker::PrintHandler::PrintHandler(const Endpoint& e, std::string topic,
                                   Callback cb, void *cookie)
    : p(new Impl{std::move(topic), cb, cookie})
	{
	// TODO: spawn actor, hook up with endpoint
	}

broker::PrintHandler::~PrintHandler() = default;

const std::string& broker::PrintHandler::Topic() const
	{
	return p->topic;
	}
