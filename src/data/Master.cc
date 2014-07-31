#include "MasterImpl.hh"
#include "../EndpointImpl.hh"
#include "../Subscription.hh"
#include "MasterActor.hh"

#include <caf/send.hpp>
#include <caf/spawn.hpp>

broker::data::Master::Master(const Endpoint& e, std::string topic,
                             std::unique_ptr<Store> s)
    : broker::data::Facade(e, topic),
      p(new Impl{caf::spawn<MasterActor>(std::move(s), topic)})
	{
	caf::anon_send(e.p->endpoint, caf::atom("sub"),
	               SubscriptionTopic{SubscriptionType::DATA_REQUEST, topic},
	               p->master);
	caf::anon_send(e.p->endpoint, caf::atom("sub"),
	               SubscriptionTopic{SubscriptionType::DATA_UPDATE, topic},
	               p->master);
	}

broker::data::Master::~Master()
	{
	caf::anon_send(p->master, caf::atom("quit"));
	}


void* broker::data::Master::GetBackendHandle() const
	{
	return &p->master;
	}
