#include "PeerImpl.hh"

broker::Peer::Peer()
    : p(new Impl{})
	{
	}

broker::Peer::Peer(const Peer& other)
    : p(new Impl{*other.p})
	{
	}

broker::Peer& broker::Peer::operator=(const Peer& other)
	{
	if ( this != &other )
		p.reset(new Impl{*other.p});

	return *this;
	}

broker::Peer::~Peer() = default;

bool broker::Peer::Valid() const
	{
	return p->endpoint != cppa::invalid_actor;
	}

bool broker::Peer::Remote() const
	{
	return p->remote;
	}

const std::pair<std::string, uint16_t>& broker::Peer::RemoteTuple() const
	{
	return p->remote_tuple;
	}
