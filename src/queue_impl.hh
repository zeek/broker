#ifndef BROKER_QUEUE_IMPL_HH
#define BROKER_QUEUE_IMPL_HH

#include <caf/actor_system.hpp>
#include <caf/send.hpp>

#include "broker/queue.hh"

#include "util/flare.hh"
#include "util/queue_actor.hh"

namespace broker {

extern std::unique_ptr<caf::actor_system> broker_system;

template <class T>
class queue<T>::impl {
public:

	impl()
    : self{*broker_system}
		{
		util::flare f;
		fd = f.fd();
		actor = broker_system->spawn<
              util::queue_actor<T>,
              caf::priority_aware
            >(std::move(f));
		// FIXME: do not rely on private API.
		self->planned_exit_reason(caf::exit_reason::unknown);
		actor->link_to(self);
		}

	int fd;
	caf::scoped_actor self;
	caf::actor actor;
};

} // namespace broker

#endif // BROKER_QUEUE_IMPL_HH
