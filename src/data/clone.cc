#include "clone_impl.hh"

broker::data::clone::clone(const endpoint& e, std::string topic)
	: broker::data::frontend(e, topic),
      pimpl(std::make_shared<impl>(*static_cast<caf::actor*>(e.handle()),
                                   std::move(topic)))
	{
	}

void* broker::data::clone::handle() const
	{
	return &pimpl->actor;
	}
