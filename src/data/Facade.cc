#include "FacadeImpl.hh"

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
