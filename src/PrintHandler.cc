#include "PrintHandlerImpl.hh"
#include "EndpointImpl.hh"
#include "PrintSubscriberActor.hh"
#include "Subscription.hh"

broker::PrintHandler::PrintHandler(const Endpoint& e, std::string topic,
                                   Callback cb, void *cookie)
    : p(new Impl{topic,
                 cppa::spawn<broker::PrintSubscriberActor>(topic, cb, cookie)})
	{
	cppa::anon_send(e.p->endpoint, cppa::atom("sub"),
	                SubscriptionTopic{SubscriptionType::PRINT, topic},
	                p->subscriber);
	}

broker::PrintHandler::~PrintHandler()
	{
	cppa::anon_send(p->subscriber, cppa::atom("quit"));
	}

const std::string& broker::PrintHandler::Topic() const
	{
	return p->topic;
	}
