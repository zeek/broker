#include "CloneImpl.hh"
#include "../EndpointImpl.hh"
#include "CloneActor.hh"

broker::data::Clone::Clone(const Endpoint &e, std::string topic)
	: broker::data::Facade(e, topic),
      p(new Impl{cppa::spawn<CloneActor>(e.p->endpoint, topic)})
	{
	}

broker::data::Clone::~Clone()
	{
	cppa::anon_send(p->clone, cppa::atom("quit"));
	}

void* broker::data::Clone::GetBackendHandle() const
	{
	return &p->clone;
	}
