#include "CloneImpl.hh"
#include "../EndpointImpl.hh"
#include "CloneActor.hh"

#include <caf/send.hpp>
#include <caf/spawn.hpp>

broker::data::Clone::Clone(const Endpoint &e, std::string topic)
	: broker::data::Facade(e, topic),
      p(new Impl{caf::spawn<CloneActor>(e.p->endpoint, topic)})
	{
	}

broker::data::Clone::~Clone()
	{
	caf::anon_send(p->clone, caf::atom("quit"));
	}

void* broker::data::Clone::GetBackendHandle() const
	{
	return &p->clone;
	}
