#include "peering_impl.hh"
#include <caf/scoped_actor.hpp>

broker::peering::peering()
    : pimpl(new impl)
	{
	}

broker::peering::peering(std::unique_ptr<impl> p)
	: pimpl(std::move(p))
	{
	}

broker::peering::~peering() = default;

broker::peering::peering(const peering& other)
	: pimpl(new impl(*other.pimpl.get()))
	{
	}

broker::peering::peering(peering&& other) = default;

broker::peering& broker::peering::operator=(const peering& other)
	{
	pimpl.reset(new impl(*other.pimpl.get()));
	return *this;
	}

broker::peering& broker::peering::operator=(peering&& other) = default;

broker::peering::operator bool() const
	{
	return pimpl->peer_actor;
	}

bool broker::peering::remote() const
	{
	return pimpl->remote;
	}

const std::pair<std::string, uint16_t>& broker::peering::remote_tuple() const
	{
	return pimpl->remote_tuple;
	}

static void handshake(const caf::actor& peer_actor,
                      const caf::actor& endpoint_actor,
                      const caf::message_handler& handler)
	{
	caf::scoped_actor self;
	self->send(peer_actor, caf::atom("handshake"), endpoint_actor, self);
	self->receive(handler);
	}

void broker::peering::handshake() const
	{
	if ( ! * this )
		return;

	caf::message_handler mh{caf::others() >> [] {}};
	::handshake(pimpl->peer_actor, pimpl->endpoint_actor, mh);
	}

bool broker::peering::handshake(std::chrono::duration<double> timeout) const
	{
	if ( ! *this )
		return true;

	bool rval = false;
	caf::message_handler mh{caf::others() >> [&rval] { rval = true; },
	                        caf::after(timeout) >> [] {}};
	::handshake(pimpl->peer_actor, pimpl->endpoint_actor, mh);
	return rval;
	}

bool broker::peering::operator==(const peering& rhs) const
	{ return pimpl == rhs.pimpl; }

template <typename T>
static inline void hash_combine(size_t& seed, const T& v)
	{ seed ^= std::hash<T>()(v) + 0x9e3779b9 + (seed<<6) + (seed>>2); }

size_t std::hash<broker::peering>::operator()(const broker::peering& p) const
	{
	size_t rval = 0;
	hash_combine(rval, p.pimpl->endpoint_actor);
	hash_combine(rval, p.pimpl->peer_actor);
	return rval;
	}
