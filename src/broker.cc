#include "broker/broker.hh"
#include "broker/data/Store.hh"
#include "Subscription.hh"
#include "data/RequestMsgs.hh"

#include <cppa/cppa.hpp>

#include <cstdio>

int broker::init(int flags)
	{
	cppa::announce<SubscriptionType>();
	cppa::announce<SubscriptionTopic>(
	            std::make_pair(&SubscriptionTopic::get_type,
	                           &SubscriptionTopic::set_type),
	            &SubscriptionTopic::topic);
	cppa::announce(typeid(Subscriptions),
	     std::unique_ptr<cppa::uniform_type_info>{new Subscriptions_type_info});
	cppa::announce<Subscriber>(&Subscriber::first, &Subscriber::second);
	cppa::announce<data::SequenceNum>(&data::SequenceNum::sequence);
	cppa::announce<data::StoreSnapshot>(&data::StoreSnapshot::store,
	                              &data::StoreSnapshot::sn);
	cppa::announce<data::SnapshotRequest>(&data::SnapshotRequest::st,
	                                      &data::SnapshotRequest::clone);
	cppa::announce<data::LookupRequest>(&data::LookupRequest::st,
	                                    &data::LookupRequest::key);
	cppa::announce<data::HasKeyRequest>(&data::HasKeyRequest::st,
	                                    &data::HasKeyRequest::key);
	cppa::announce<data::KeysRequest>(&data::KeysRequest::st);
	cppa::announce<data::SizeRequest>(&data::SizeRequest::st);
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
		return ::strerror(arg_errno);
	};
	}

const char* broker_strerror(int arg_errno)
	{
	return broker::strerror(arg_errno);
	}
