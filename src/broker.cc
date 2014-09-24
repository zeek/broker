#include "broker/broker.hh"
#include "broker/store/store.hh"
#include "broker/print_msg.hh"
#include "broker/print_queue.hh"
#include "broker/store/response_queue.hh"
#include "store/result_type_info.hh"
#include "data_type_info.hh"
#include "subscription.hh"
#include <caf/announce.hpp>
#include <caf/shutdown.hpp>
#include <cstdio>

int broker::init(int flags)
	{
	// TODO: need a better, more organized way to announce types.
	using caf::announce;
	using namespace std;
	using namespace broker::store;
	announce<subscription_type>();
	announce<subscription>(&subscription::type, &subscription::topic);
	announce(typeid(subscriptions),
	         unique_ptr<caf::uniform_type_info>(new subscriptions_type_info));
	announce<subscriber>(&subscriber::first, &subscriber::second);
	announce<sequence_num>(&sequence_num::sequence);
	announce<snapshot>(&snapshot::datastore, &snapshot::sn);
	announce<data::tag>();
	announce(typeid(data),
	         unique_ptr<caf::uniform_type_info>(new data_type_info));
	announce<std::unordered_set<data>>();
	announce<std::deque<std::string>>();
	announce<result::type>();
	announce<result::status>();
	announce(typeid(result),
	         unique_ptr<caf::uniform_type_info>(new result_type_info));
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
