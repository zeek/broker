#include "broker/broker.hh"
#include "endpoint_impl.hh"
#include "subscription.hh"
#include <caf/io/publish.hpp>
#include <caf/send.hpp>

static inline caf::actor& handle_to_actor(void* h)
	{ return *static_cast<caf::actor*>(h); }

broker::endpoint::endpoint(std::string name, int flags)
    : pimpl(new impl(std::move(name)))
	{
	}

broker::endpoint::~endpoint() = default;

broker::endpoint::endpoint(endpoint&& other) = default;

broker::endpoint& broker::endpoint::operator=(endpoint&& other) = default;

const std::string& broker::endpoint::name() const
	{
	return pimpl->name;
	}

int broker::endpoint::last_errno() const
	{
	return pimpl->last_errno;
	}

const std::string& broker::endpoint::last_error() const
	{
	return pimpl->last_error;
	}

bool broker::endpoint::listen(uint16_t port, const char* addr)
	{
	try
		{
		caf::io::publish(pimpl->actor, port, addr);
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
		caf::anon_send(rval.pimpl->peer_actor, caf::atom("peerstat"));
	else
		{
		auto psa = handle_to_actor(pimpl->peer_status.handle());
		auto a = caf::spawn<endpoint_proxy_actor>(pimpl->actor, addr, port,
		                                          retry, psa);
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
	caf::anon_send(pimpl->actor, caf::atom("peer"), e.pimpl->actor,
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
		caf::anon_send(p.pimpl->peer_actor, caf::atom("quit"));
	else
		{
		caf::anon_send(pimpl->actor, caf::atom("unpeer"), p.pimpl->peer_actor);
		caf::anon_send(p.pimpl->peer_actor, caf::atom("unpeer"), pimpl->actor);
		}

	return true;
	}

const broker::peer_status_queue& broker::endpoint::peer_status() const
	{
	return pimpl->peer_status;
	}

void broker::endpoint::print(std::string topic, print_msg msg) const
	{
	caf::anon_send(pimpl->actor,
	               subscription{subscription_type::print, std::move(topic)},
	               std::move(msg));
	}

void broker::endpoint::log(std::string topic, log_msg msg) const
	{
	caf::anon_send(pimpl->actor,
	               subscription{subscription_type::log, std::move(topic)},
	               std::move(msg));
	}

void broker::endpoint::event(std::string topic, event_msg msg) const
	{
	caf::anon_send(pimpl->actor,
	               subscription{subscription_type::event, std::move(topic)},
	               std::move(msg));
	}

void* broker::endpoint::handle() const
	{
	return &pimpl->actor;
	}
