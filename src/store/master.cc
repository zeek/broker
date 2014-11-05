#include "master_impl.hh"

broker::store::master::master(const endpoint& e, identifier name,
                              std::unique_ptr<backend> s)
    : broker::store::frontend(e, name),
      pimpl(new impl(*static_cast<caf::actor*>(e.handle()),
                     std::move(name), std::move(s)))
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
