#include "MasterImpl.hh"
#include "../EndpointImpl.hh"
#include "../Subscription.hh"
#include "MasterActor.hh"

broker::data::Master::Master(const Endpoint& e, std::string topic,
                             std::unique_ptr<Store> s)
    : broker::data::Facade(e, topic),
      p(new Impl{cppa::spawn<MasterActor>(std::move(s), topic)})
	{
	cppa::anon_send(e.p->endpoint, cppa::atom("sub"),
	                SubscriptionTopic{SubscriptionType::DATA_REQUEST, topic},
	                p->master);
	cppa::anon_send(e.p->endpoint, cppa::atom("sub"),
	                SubscriptionTopic{SubscriptionType::DATA_UPDATE, topic},
	                p->master);
	}

broker::data::Master::~Master()
	{
	cppa::anon_send(p->master, cppa::atom("quit"));
	}


void* broker::data::Master::GetBackendHandle() const
	{
	return &p->master;
	}
