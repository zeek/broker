#include "master_impl.hh"

broker::data::master::master(const endpoint& e, std::string topic,
                             std::unique_ptr<store> s)
    : broker::data::frontend(e, topic),
      pimpl(std::make_shared<impl>(*static_cast<caf::actor*>(e.handle()),
                                   std::move(topic), std::move(s)))
	{
	}

void* broker::data::master::handle() const
	{
	return &pimpl->actor;
	}
