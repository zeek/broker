#include "broker/broker.hh"
#include "broker/data/store.hh"
#include "broker/print_msg.hh"
#include "broker/print_queue.hh"
#include "broker/data/response_queue.hh"
#include "data/result_type_info.hh"
#include "subscription.hh"
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
	announce<snapshot>(&snapshot::datastore, &snapshot::sn);
	announce<std::unordered_set<broker::data::key>>();
	announce<std::deque<std::string>>();
	announce<result::type>();
	announce<result::status>();
	announce(typeid(result),
	         unique_ptr<uniform_type_info>(new result_type_info));
	announce<query::type>();
	announce<query>(&query::tag, &query::k);
	announce<response>(&response::request, &response::reply, &response::cookie);
	announce<std::deque<response>>();
	announce<print_msg>(&print_msg::path, &print_msg::data);
	announce<std::deque<print_msg>>();
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

const char* broker::strerror(int broker_errno)
	{
	switch ( broker_errno ) {
	default:
		return ::strerror(broker_errno);
	}
	}

const char* broker_strerror(int arg_errno)
	{
	return broker::strerror(arg_errno);
	}

int broker::strerror_r(int broker_errno, char* buf, size_t len)
	{
	switch ( broker_errno ) {
	default:
		return ::strerror_r(broker_errno, buf, len);
	}
	}

int broker_strerror_r(int broker_errno, char* buf, size_t buflen)
	{
	return broker::strerror_r(broker_errno, buf, buflen);
	}
