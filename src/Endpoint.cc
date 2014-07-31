#include "broker/broker.hh"
#include "EndpointImpl.hh"
#include "PeerImpl.hh"
#include "EndpointActor.hh"
#include "EndpointProxyActor.hh"
#include "Subscription.hh"

#include <caf/io/publish.hpp>
#include <caf/send.hpp>

broker::Endpoint::Endpoint(std::string name, int flags)
    : p(new Impl{std::move(name), caf::spawn<broker::EndpointActor>()})
	{
	}

broker::Endpoint::~Endpoint()
	{
	caf::anon_send(p->endpoint, caf::atom("quit"));

	for ( const auto& peer : p->peers )
		if ( peer.second.Remote() )
			caf::anon_send(peer.second.p->endpoint, caf::atom("quit"));
	}

const std::string& broker::Endpoint::Name() const
	{
	return p->name;
	}

int broker::Endpoint::LastErrno() const
	{
	return p->last_errno;
	}

const std::string& broker::Endpoint::LastError() const
	{
	return p->last_error;
	}

bool broker::Endpoint::Listen(uint16_t port, const char* addr)
	{
	try
		{
		caf::io::publish(p->endpoint, port, addr);
		}
	catch ( const std::exception& e )
		{
		p->last_errno = 0;
		p->last_error = e.what();
		return false;
		}

	return true;
	}

broker::Peer broker::Endpoint::AddPeer(std::string addr, uint16_t port,
                                       std::chrono::duration<double> retry)
	{
	auto port_addr = std::pair<std::string, uint16_t>(addr, port);

	for ( const auto& peer : p->peers )
		if ( peer.second.Remote() && port_addr == peer.second.RemoteTuple() )
			return peer.second;

	auto peer = caf::spawn<EndpointProxyActor>(p->endpoint, addr, port, retry);

	Peer rval;
	rval.p->endpoint = peer;
	rval.p->remote = true;
	rval.p->remote_tuple = port_addr;
	p->peers[peer] = rval;
	// The proxy actor will initiate peer requests once connected.
	return rval;
	}

broker::Peer broker::Endpoint::AddPeer(const Endpoint& e)
	{
	if ( this == &e )
		return {};

	auto it = p->peers.find(e.p->endpoint);

	if ( it != p->peers.end() )
		return it->second;

	Peer rval;
	rval.p->endpoint = e.p->endpoint;
	p->peers[e.p->endpoint] = rval;
	caf::anon_send(p->endpoint, caf::atom("peer"), e.p->endpoint);
	return rval;
	}

bool broker::Endpoint::RemPeer(broker::Peer peer)
	{
	if ( ! peer.Valid() )
		return false;

	auto it = p->peers.find(peer.p->endpoint);

	if ( it == p->peers.end() )
		return false;

	p->peers.erase(it);

	if ( peer.Remote() )
		// The proxy actor initiates unpeer messages.
		caf::anon_send(peer.p->endpoint, caf::atom("quit"));
	else
		caf::anon_send(p->endpoint, caf::atom("unpeer"), peer.p->endpoint);

	return true;
	}

void broker::Endpoint::Print(std::string topic, std::string msg) const
	{
	SubscriptionTopic st{SubscriptionType::PRINT, std::move(topic)};
	caf::anon_send(p->endpoint, std::move(st), std::move(msg));
	}
