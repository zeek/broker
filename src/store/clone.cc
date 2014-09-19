#include "clone_impl.hh"

broker::store::clone::clone(const endpoint& e, std::string topic,
                            std::chrono::duration<double> resync_interval)
	: broker::store::frontend(e, topic),
      pimpl(new impl(*static_cast<caf::actor*>(e.handle()),
                     std::move(topic), std::move(resync_interval)))
	{
	}

broker::store::clone::~clone() = default;

broker::store::clone::clone(clone&& other) = default;

broker::store::clone& broker::store::clone::operator=(clone&& other) = default;

void* broker::store::clone::handle() const
	{
	return &pimpl->actor;
	}
