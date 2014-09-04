#include "master_impl.hh"

broker::data::master::master(const endpoint& e, std::string topic,
                             std::unique_ptr<store> s)
    : broker::data::frontend(e, topic),
      pimpl(new impl(*static_cast<caf::actor*>(e.handle()),
                     std::move(topic), std::move(s)))
	{
	}

broker::data::master::~master() = default;

broker::data::master::master(master&& other) = default;

broker::data::master& broker::data::master::operator=(master&& other) = default;

void* broker::data::master::handle() const
	{
	return &pimpl->actor;
	}
