#include "broker/broker.hh"
#include "broker/peer_status.hh"
#include "broker/print_msg.hh"
#include "broker/log_msg.hh"
#include "broker/event_msg.hh"
#include "broker/store/store.hh"
#include "broker/store/query.hh"
#include "broker/store/response.hh"
#include "store/result_type_info.hh"
#include "data_type_info.hh"
#include "peering_impl.hh"
#include "peering_type_info.hh"
#include "subscription.hh"
#include <caf/announce.hpp>
#include <caf/shutdown.hpp>
#include <cstdio>
#include <deque>

int broker_init(int flags)
	{
	// TODO: need a better, more organized way to announce types.
	using caf::announce;
	using namespace std;
	using namespace broker;
	using namespace broker::store;
	announce<peer_status::type>();
	announce<peer_status>(&peer_status::relation, &peer_status::status);
	announce(typeid(peering),
	         unique_ptr<caf::uniform_type_info>(new peering_type_info));
	announce<peering::impl>(&peering::impl::endpoint_actor,
	                        &peering::impl::peer_actor, &peering::impl::remote,
	                        &peering::impl::remote_tuple);
	announce<subscription_type>();
	announce<subscription>(&subscription::type, &subscription::topic);
	announce(typeid(subscriptions),
	         unique_ptr<caf::uniform_type_info>(new subscriptions_type_info));
	announce<subscriber>(&subscriber::first, &subscriber::second);
	announce<sequence_num>(&sequence_num::sequence);
	announce<snapshot>(&snapshot::datastore, &snapshot::sn);
	announce<data::tag>();
	announce<record>(&record::fields);
	announce(typeid(util::optional<data>),
	         unique_ptr<caf::uniform_type_info>(new optional_data_type_info));
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
	announce<log_msg>(&log_msg::stream, &log_msg::fields);
	announce<event_msg>(&event_msg::name, &event_msg::args);
	announce<std::deque<print_msg>>();
	announce<std::deque<log_msg>>();
	announce<std::deque<event_msg>>();
	announce<std::deque<peer_status>>();
	return 0;
	}

int broker::init(int flags)
	{ return broker_init(flags); }

void broker_done()
	{ caf::shutdown(); }

const char* broker_strerror(int broker_errno)
	{
	switch ( broker_errno ) {
	default:
		return ::strerror(broker_errno);
	}
	}

int broker_strerror_r(int broker_errno, char* buf, size_t len)
	{
	switch ( broker_errno ) {
	default:
		return ::strerror_r(broker_errno, buf, len);
	}
	}
