#include "broker/broker.hh"
#include "endpoint_impl.hh"
#include <caf/io/publish.hpp>
#include <caf/send.hpp>

static inline caf::actor& handle_to_actor(void* h)
	{ return *static_cast<caf::actor*>(h); }

broker::endpoint::endpoint(std::string name, int flags)
    : pimpl(new impl(this, std::move(name), flags))
	{
	}

broker::endpoint::~endpoint() = default;

broker::endpoint::endpoint(endpoint&& other) = default;

broker::endpoint& broker::endpoint::operator=(endpoint&& other) = default;

const std::string& broker::endpoint::name() const
	{
	return pimpl->name;
	}

int broker::endpoint::flags() const
	{
	return pimpl->flags;
	}

void broker::endpoint::set_flags(int flags)
	{
	pimpl->flags = flags;
	caf::anon_send(pimpl->actor, flags_atom::value, flags);
	}

int broker::endpoint::last_errno() const
	{
	return pimpl->last_errno;
	}

const std::string& broker::endpoint::last_error() const
	{
	return pimpl->last_error;
	}

bool broker::endpoint::listen(uint16_t port, const char* addr, bool reuse_addr)
	{
	try
		{
		caf::io::publish(pimpl->actor, port, addr, reuse_addr);
		}
	catch ( const std::exception& e )
		{
		pimpl->last_errno = 0;
		pimpl->last_error = e.what();
		return false;
		}

	return true;
	}

broker::peering broker::endpoint::peer(std::string addr, uint16_t port,
                                       std::chrono::duration<double> retry)
	{
	auto port_addr = std::pair<std::string, uint16_t>(addr, port);
	peering rval;

	for ( const auto& peer : pimpl->peers )
		if ( peer.remote() && port_addr == peer.remote_tuple() )
			{
			rval = peer;
			break;
			}

	if ( rval )
		caf::anon_send(rval.pimpl->peer_actor, peerstat_atom::value);
	else
		{
		auto h = handle_to_actor(pimpl->outgoing_conns.handle());
		auto a = caf::spawn<endpoint_proxy_actor>(pimpl->actor, pimpl->name,
		                                          addr, port, retry, h);
		a->link_to(pimpl->self);
		rval = peering(std::unique_ptr<peering::impl>(
	                   new peering::impl(pimpl->actor, std::move(a),
	                                     true, port_addr)));
		pimpl->peers.insert(rval);
		}

	return rval;
	}

broker::peering broker::endpoint::peer(const endpoint& e)
	{
	if ( this == &e )
		return {};

	peering p(std::unique_ptr<peering::impl>(
	              new peering::impl(pimpl->actor, e.pimpl->actor)));
	pimpl->peers.insert(p);
	caf::anon_send(pimpl->actor, peer_atom::value, e.pimpl->actor,
	               *p.pimpl.get());
	return p;
	}

bool broker::endpoint::unpeer(broker::peering p)
	{
	if ( ! p )
		return false;

	auto it = pimpl->peers.find(p);

	if ( it == pimpl->peers.end() )
		return false;

	pimpl->peers.erase(it);

	if ( p.remote() )
		// The proxy actor initiates unpeer messages.
		caf::anon_send(p.pimpl->peer_actor, quit_atom::value);
	else
		{
		caf::anon_send(pimpl->actor, unpeer_atom::value, p.pimpl->peer_actor);
		caf::anon_send(p.pimpl->peer_actor, unpeer_atom::value, pimpl->actor);
		}

	return true;
	}

const broker::outgoing_connection_status_queue&
broker::endpoint::outgoing_connection_status() const
	{
	return pimpl->outgoing_conns;
	}

const broker::incoming_connection_status_queue&
broker::endpoint::incoming_connection_status() const
	{
	return pimpl->incoming_conns;
	}

void broker::endpoint::send(topic t, message msg, int flags) const
	{
	caf::anon_send(pimpl->actor, std::move(t), std::move(msg), flags);
	}

void broker::endpoint::publish(topic t)
	{
	caf::anon_send(pimpl->actor, acl_pub_atom::value, t);
	}

void broker::endpoint::unpublish(topic t)
	{
	caf::anon_send(pimpl->actor, acl_unpub_atom::value, t);
	}

void broker::endpoint::advertise(topic t)
	{
	caf::anon_send(pimpl->actor, advert_atom::value, t);
	}

void broker::endpoint::unadvertise(topic t)
	{
	caf::anon_send(pimpl->actor, unadvert_atom::value, t);
	}

void* broker::endpoint::handle() const
	{
	return &pimpl->actor;
	}
