#include "MasterImpl.hh"

broker::data::Master::Master(const Endpoint& e, std::string topic, Store s)
    : broker::data::Facade(e, std::move(topic)), p(new Impl{})
	{
	// TODO
	}

broker::data::Master::~Master() = default;
