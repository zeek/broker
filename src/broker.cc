#include "broker/broker.hh"
#include "broker/data/store.hh"
#include "broker/print_queue.hh"
#include "subscription.hh"
#include "data/query_types.hh"

#include <caf/announce.hpp>
#include <caf/shutdown.hpp>

#include <cstdio>

int broker::init(int flags)
	{
	// TODO: need a better, more organized way to announce types.
	using namespace caf;
	using namespace std;
	using namespace broker::data;
	announce<subscription_type>();
	announce<subscription>(&subscription::type, &subscription::topic);
	announce(typeid(subscriptions),
	         unique_ptr<uniform_type_info>(new subscriptions_type_info));
	announce<subscriber>(&subscriber::first, &subscriber::second);
	announce<sequence_num>(&sequence_num::sequence);
	announce<store_snapshot>(&store_snapshot::datastore, &store_snapshot::sn);
	announce<snapshot_request>(&snapshot_request::st, &snapshot_request::clone);
	announce<lookup_request>(&lookup_request::st, &lookup_request::key);
	announce<has_key_request>(&has_key_request::st, &has_key_request::key);
	announce<keys_request>(&keys_request::st);
	announce<size_request>(&size_request::st);
	announce<std::unordered_set<broker::data::key>>();
	announce<std::deque<std::string>>();
	return 0;
	}

int broker_init(int flags)
	{
	return broker::init(flags);
	}

void broker::done()
	{
	caf::shutdown();
	}

void broker_done()
	{
	return broker::done();
	}

const char* broker::strerror(int arg_errno)
	{
	switch ( arg_errno ) {
	default:
		return ::strerror(arg_errno);
	};
	}

const char* broker_strerror(int arg_errno)
	{
	return broker::strerror(arg_errno);
	}
