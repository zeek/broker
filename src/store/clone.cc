#include "clone_impl.hh"

broker::store::clone::clone(const endpoint& e, identifier master_name,
                            std::chrono::duration<double> ri,
                            std::unique_ptr<backend> b)
	: broker::store::frontend(e, master_name),
      pimpl(new impl(*static_cast<caf::actor*>(e.handle()),
                     std::move(master_name),
                     std::chrono::duration_cast<std::chrono::microseconds>(ri),
                     std::move(b)))
	{
	}

broker::store::clone::~clone() = default;

broker::store::clone::clone(clone&& other) = default;

broker::store::clone& broker::store::clone::operator=(clone&& other) = default;

void* broker::store::clone::handle() const
	{
	return &pimpl->actor;
	}
