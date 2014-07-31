#include "PrintHandlerImpl.hh"
#include "EndpointImpl.hh"
#include "PrintSubscriberActor.hh"
#include "Subscription.hh"

#include <caf/spawn.hpp>
#include <caf/send.hpp>

broker::PrintHandler::PrintHandler(const Endpoint& e, std::string topic,
                                   Callback cb, void *cookie)
    : p(new Impl{topic,
                 caf::spawn<broker::PrintSubscriberActor>(topic, cb, cookie)})
	{
	caf::anon_send(e.p->endpoint, caf::atom("sub"),
	               SubscriptionTopic{SubscriptionType::PRINT, topic},
	               p->subscriber);
	}

broker::PrintHandler::~PrintHandler()
	{
	caf::anon_send(p->subscriber, caf::atom("quit"));
	}

const std::string& broker::PrintHandler::Topic() const
	{
	return p->topic;
	}
