#include "peering_impl.hh"
#include "broker/util/hash.hh"
#include <caf/scoped_actor.hpp>
#include <iostream>

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

bool broker::peering::operator==(const peering& rhs) const
	{ return *pimpl == *rhs.pimpl; }

size_t std::hash<broker::peering>::operator()(const broker::peering& p) const
	{
	size_t rval = 0;
	broker::util::hash_combine(rval, p.pimpl->endpoint_actor);
	broker::util::hash_combine(rval, p.pimpl->peer_actor);
	return rval;
	}

// Begin C API
#include "broker/broker.h"
using std::nothrow;

broker_peering* broker_peering_create()
	{
	return reinterpret_cast<broker_peering*>(new (nothrow) broker::peering());
	}

void broker_peering_delete(broker_peering* p)
	{ delete reinterpret_cast<broker::peering*>(p); }

broker_peering* broker_peering_copy(const broker_peering* p)
	{
	auto pp = reinterpret_cast<const broker::peering*>(p);
	auto rval = new (nothrow) broker::peering(*pp);
	return reinterpret_cast<broker_peering*>(rval);
	}

int broker_peering_is_remote(const broker_peering* p)
	{
	auto pp = reinterpret_cast<const broker::peering*>(p);
	return pp->remote();
	}

const char* broker_peering_remote_host(const broker_peering* p)
	{
	auto pp = reinterpret_cast<const broker::peering*>(p);
	return pp->remote_tuple().first.data();
	}

uint16_t broker_peering_remote_port(const broker_peering* p)
	{
	auto pp = reinterpret_cast<const broker::peering*>(p);
	return pp->remote_tuple().second;
	}

int broker_peering_is_initialized(const broker_peering* p)
	{
	auto pp = reinterpret_cast<const broker::peering*>(p);
	bool rval(*pp);
	return rval;
	}

int broker_peering_eq(const broker_peering* a, const broker_peering* b)
	{
	auto aa = reinterpret_cast<const broker::peering*>(a);
	auto bb = reinterpret_cast<const broker::peering*>(b);
	return *aa == *bb;
	}

size_t broker_peering_hash(const broker_peering* a)
	{
	auto aa = reinterpret_cast<const broker::peering*>(a);
	return std::hash<broker::peering>{}(*aa);
	}
