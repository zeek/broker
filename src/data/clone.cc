#include "clone_impl.hh"

broker::data::clone::clone(const endpoint& e, std::string topic,
                           std::chrono::duration<double> resync_interval)
	: broker::data::frontend(e, topic),
      pimpl(new impl(*static_cast<caf::actor*>(e.handle()),
                     std::move(topic), std::move(resync_interval)))
	{
	}

broker::data::clone::~clone() = default;

broker::data::clone::clone(clone&& other) = default;

broker::data::clone& broker::data::clone::operator=(clone&& other) = default;

void* broker::data::clone::handle() const
	{
	return &pimpl->actor;
	}
