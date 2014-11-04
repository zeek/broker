#include "queue_impl.hh"

template <class T>
broker::queue<T>::queue()
	: pimpl(new impl)
	{}

template <class T>
broker::queue<T>::~queue() = default;

template <class T>
broker::queue<T>::queue(queue<T>&&) = default;

template <class T>
broker::queue<T>& broker::queue<T>::operator=(queue<T>&&) = default;

template <class T>
int broker::queue<T>::fd() const
	{ return pimpl->fd; }

template <class T>
void* broker::queue<T>::handle() const
	{ return &pimpl->actor; }

template <class T>
std::deque<T> broker::queue<T>::want_pop() const
	{ return util::queue_pop<T>(pimpl->actor, caf::atom("want")); }

template <class T>
std::deque<T> broker::queue<T>::need_pop() const
	{ return util::queue_pop<T>(pimpl->actor, caf::atom("need")); }

// Explicit template instantiations.  We need to do this if using the
// Pimpl idiom to separate/hide the implementation details.  Else, we would
// need to expose some CAF things in queue.hh.

#include "broker/message.hh"
template class broker::queue<broker::message>;

#include "broker/peer_status.hh"
template class broker::queue<broker::peer_status>;

#include "broker/store/response.hh"
template class broker::queue<broker::store::response>;
