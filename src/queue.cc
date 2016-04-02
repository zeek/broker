#include "atoms.hh"
#include "peering_impl.hh"
#include "queue_impl.hh"

namespace broker {

template <class T>
queue<T>::queue()
  : pimpl(new impl) {
}

template <class T>
queue<T>::~queue() = default;

template <class T>
queue<T>::queue(queue<T>&&) = default;

template <class T>
queue<T>& queue<T>::operator=(queue<T>&&) = default;

template <class T>
int queue<T>::fd() const {
  return pimpl->fd;
}

template <class T>
void* queue<T>::handle() const {
  return &pimpl->actor;
}

template <class T>
std::deque<T> queue<T>::want_pop() const {
  return util::queue_pop<T>(pimpl->self, pimpl->actor, want_atom::value);
}

template <class T>
std::deque<T> queue<T>::need_pop() const {
  return util::queue_pop<T>(pimpl->self, pimpl->actor, need_atom::value);
}

} // namespace broker

// Explicit template instantiations.  We need to do this if using the
// Pimpl idiom to separate/hide the implementation details.  Else, we would
// need to expose some CAF things in queue.hh.

#include "broker/message.hh"
template class broker::queue<broker::message>;

#include "broker/outgoing_connection_status.hh"
template class broker::queue<broker::outgoing_connection_status>;

#include "broker/incoming_connection_status.hh"
template class broker::queue<broker::incoming_connection_status>;

#include "broker/store/response.hh"
template class broker::queue<broker::store::response>;
