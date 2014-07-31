#include "broker/broker.hh"
#include "broker/data/Store.hh"
#include "Subscription.hh"
#include "data/RequestMsgs.hh"

#include <caf/announce.hpp>
#include <caf/shutdown.hpp>

#include <cstdio>

int broker::init(int flags)
	{
	using namespace caf;
	using namespace std;
	using namespace broker::data;
	announce<SubscriptionType>();
	announce<SubscriptionTopic>(&SubscriptionTopic::type,
	                            &SubscriptionTopic::topic);
	announce(typeid(Subscriptions),
	         unique_ptr<uniform_type_info>(new Subscriptions_type_info));
	announce<Subscriber>(&Subscriber::first, &Subscriber::second);
	announce<SequenceNum>(&SequenceNum::sequence);
	announce<StoreSnapshot>(&StoreSnapshot::store, &StoreSnapshot::sn);
	announce<SnapshotRequest>(&SnapshotRequest::st, &SnapshotRequest::clone);
	announce<LookupRequest>(&LookupRequest::st, &LookupRequest::key);
	announce<HasKeyRequest>(&HasKeyRequest::st, &HasKeyRequest::key);
	announce<KeysRequest>(&KeysRequest::st);
	announce<SizeRequest>(&SizeRequest::st);
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
