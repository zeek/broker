#include "broker/broker.hh"
#include "endpoint_impl.hh"
#include "subscription.hh"
#include <caf/io/publish.hpp>
#include <caf/send.hpp>

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

	for ( const auto& peer : pimpl->peers )
		if ( peer.remote() && port_addr == peer.remote_tuple() )
			return peer;

	auto a = caf::spawn<endpoint_proxy_actor>(pimpl->actor, addr, port, retry);
	a->link_to(pimpl->self);
	peering rval(std::unique_ptr<peering::impl>(
	                 new peering::impl(pimpl->actor, std::move(a),
	                                   true, port_addr)));
	pimpl->peers.insert(rval);
	// The proxy actor will initiate peer requests once connected.
	return rval;
	}

broker::peering broker::endpoint::peer(const endpoint& e)
	{
	if ( this == &e )
		return {};

	peering p(std::unique_ptr<peering::impl>(
	              new peering::impl(pimpl->actor, e.pimpl->actor)));

	if ( pimpl->peers.insert(p).second )
		caf::anon_send(pimpl->actor, caf::atom("peer"), e.pimpl->actor,
		               BROKER_PROTOCOL_VERSION);

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

void broker::endpoint::print(std::string topic, print_msg msg) const
	{
	caf::anon_send(pimpl->actor,
	               subscription{subscription_type::print, std::move(topic)},
	               std::move(msg));
	}

void* broker::endpoint::handle() const
	{
	return &pimpl->actor;
	}
