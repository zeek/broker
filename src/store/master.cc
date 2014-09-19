#include "master_impl.hh"

broker::store::master::master(const endpoint& e, std::string topic,
                              std::unique_ptr<store> s)
    : broker::store::frontend(e, topic),
      pimpl(new impl(*static_cast<caf::actor*>(e.handle()),
                     std::move(topic), std::move(s)))
	{
	}

broker::store::master::~master() = default;

broker::store::master::master(master&& other) = default;

broker::store::master&
broker::store::master::operator=(master&& other) = default;

void* broker::store::master::handle() const
	{
	return &pimpl->actor;
	}
