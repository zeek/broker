#include "clone_impl.hh"

broker::data::clone::clone(const endpoint& e, std::string topic,
                           std::chrono::duration<double> resync_interval)
	: broker::data::frontend(e, topic),
      pimpl(std::make_shared<impl>(*static_cast<caf::actor*>(e.handle()),
                                   std::move(topic),
                                   std::move(resync_interval)))
	{
	}

void* broker::data::clone::handle() const
	{
	return &pimpl->actor;
	}
