#include "broker/broker.hh"
#include "Subscription.hh"

#include <cppa/cppa.hpp>

#include <cstdio>

int broker::init(int flags)
	{
	cppa::announce<SubscriptionType>();
	cppa::announce<SubscriptionTopic>(
	            std::make_pair(&SubscriptionTopic::get_type,
	                           &SubscriptionTopic::set_type),
	            std::make_pair(&SubscriptionTopic::get_topic,
	                           &SubscriptionTopic::set_topic));
	cppa::announce(typeid(Subscriptions),
	     std::unique_ptr<cppa::uniform_type_info>{new Subscriptions_type_info});
	cppa::announce<Subscriber>(&Subscriber::first, &Subscriber::second);
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
