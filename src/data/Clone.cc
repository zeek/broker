#include "CloneImpl.hh"

broker::data::Clone::Clone(const Endpoint &e, std::string topic)
	: broker::data::Facade(e, std::move(topic)), p(new Impl{})
	{
	// TODO
	}

broker::data::Clone::~Clone() = default;
