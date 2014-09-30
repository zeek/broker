#include "peering_impl.hh"
#include "broker/util/hash.hh"
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

bool broker::peering::operator==(const peering& rhs) const
	{ return pimpl == rhs.pimpl; }

size_t std::hash<broker::peering>::operator()(const broker::peering& p) const
	{
	size_t rval = 0;
	broker::util::hash_combine(rval, p.pimpl->endpoint_actor);
	broker::util::hash_combine(rval, p.pimpl->peer_actor);
	return rval;
	}
