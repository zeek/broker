#include "broker/broker.hh"

#include <cppa/cppa.hpp>

#include <cstdio>

int broker::init(int flags)
	{
	// TODO: cppa announcements
	return 0;
	}

int broker_init(int flags)
	{
	return broker::init(flags);
	}

void broker::done()
	{
	cppa::shutdown();
	}

void broker_done()
	{
	return broker::done();
	}

const char* broker::strerror(int arg_errno)
	{
	switch ( arg_errno ) {
	default:
		return strerror(arg_errno);
	};
	}

const char* broker_strerror(int arg_errno)
	{
	return broker::strerror(arg_errno);
	}

class broker::Endpoint::Impl {
public:

	std::string name;
	// TODO
};

broker::Endpoint::Endpoint(std::string name, int flags)
    : p(new Impl{std::move(name)})
	{
	// TODO: spawn some actor
	}

broker::Endpoint::~Endpoint() = default;

int broker::Endpoint::Listen(std::string addr, uint16_t port)
	{
	// TODO
	return 0;
	}

broker::PeerNum broker::Endpoint::AddPeer(std::string addr, uint16_t port,
                                          std::chrono::duration<double> retry)
	{
	// TODO
	return 0;
	}

broker::PeerNum broker::Endpoint::AddPeer(const Endpoint& e)
	{
	// TODO
	return 0;
	}

bool broker::Endpoint::RemPeer(broker::PeerNum peernum)
	{
	// TODO
	return false;
	}

void broker::Endpoint::Print(const std::string& topic,
                             const std::string& msg) const
	{
	// TODO
	}

class broker::PrintHandler::Impl {
public:

	std::string topic;
	broker::PrintHandler::Callback cb;
	void* cookie;
};

broker::PrintHandler::PrintHandler(const Endpoint& e, std::string topic,
                                   Callback cb, void *cookie)
    : p(new Impl{std::move(topic), cb, cookie})
	{
	// TODO: spawn actor, hook up with endpoint
	}

broker::PrintHandler::~PrintHandler() = default;

const std::string& broker::PrintHandler::Topic() const
	{
	return p->topic;
	}

class broker::data::Facade::Impl {
public:

	std::string topic;
	// TODO
};

broker::data::Facade::Facade(const Endpoint &e, std::string topic)
    : p(new Impl{std::move(topic)})
	{
	// TODO
	}

broker::data::Facade::~Facade() = default;

const std::string& broker::data::Facade::Topic() const
	{
	return p->topic;
	}

void broker::data::Facade::Insert(Key k, Val v) const
	{
	// TODO
	}

void broker::data::Facade::Erase(Key k) const
	{
	// TODO
	}

void broker::data::Facade::Clear() const
	{
	// TODO
	}

std::unique_ptr<broker::data::Val> broker::data::Facade::Lookup(Key k) const
	{
	// TODO
	return {};
	}

std::unordered_set<broker::data::Key> broker::data::Facade::Keys() const
	{
	// TODO
	return {};
	}

uint64_t broker::data::Facade::Size() const
	{
	// TODO
	return 0;
	}

void broker::data::Facade::Lookup(Key k, LookupCallback cb, void* cookie) const
	{
	// TODO
	}

void broker::data::Facade::Keys(KeysCallback cb, void* cookie) const
	{
	// TODO
	}

void broker::data::Facade::Size(SizeCallback cb, void* cookie) const
	{
	// TODO
	}

class broker::data::Master::Impl {
public:
	// TODO:
};

broker::data::Master::Master(const Endpoint& e, std::string topic, Store s)
    : broker::data::Facade(e, std::move(topic)), p(new Impl{})
	{
	// TODO
	}

broker::data::Master::~Master() = default;

class broker::data::Clone::Impl {
public:
	// TODO
};

broker::data::Clone::Clone(const Endpoint &e, std::string topic)
	: broker::data::Facade(e, std::move(topic)), p(new Impl{})
	{
	// TODO
	}

broker::data::Clone::~Clone() = default;
