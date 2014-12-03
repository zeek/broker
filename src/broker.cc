#include "broker/broker.hh"
#include "broker/report.hh"
#include "broker/store/backend.hh"
#include "broker/store/query.hh"
#include "broker/store/response.hh"
#include "broker/store/expiration_time.hh"
#include "store/result_type_info.hh"
#include "data_type_info.hh"
#include "peering_impl.hh"
#include "peering_type_info.hh"
#include "subscription.hh"
#include <caf/announce.hpp>
#include <caf/shutdown.hpp>
#include <cstdio>
#include <deque>

namespace broker { namespace report {
std::unique_ptr<broker::endpoint> manager;
std::unique_ptr<broker::message_queue> default_queue;
}}

int broker_init(int flags)
	{
	// TODO: need a better, more organized way to announce types.
	using caf::announce;
	using namespace std;
	using namespace broker;
	using namespace broker::store;

	announce(typeid(topic_set),
	         unique_ptr<caf::uniform_type_info>(new topic_set_type_info));
	announce<peer_status::tag>();
	announce<peer_status>(&peer_status::relation, &peer_status::status,
	                      &peer_status::peer_name);
	announce(typeid(peering),
	         unique_ptr<caf::uniform_type_info>(new peering_type_info));
	announce<peering::impl>(&peering::impl::endpoint_actor,
	                        &peering::impl::peer_actor, &peering::impl::remote,
	                        &peering::impl::remote_tuple);
	announce<sequence_num>(&sequence_num::sequence);
	announce<snapshot>(&snapshot::datastore, &snapshot::sn);
	announce<data::tag>();
	announce(typeid(data),
	         unique_ptr<caf::uniform_type_info>(new data_type_info));
	announce(typeid(util::optional<data>),
	         unique_ptr<caf::uniform_type_info>(new optional_data_type_info));
	announce<record>(&record::fields);
	announce<std::unordered_set<data>>();
	announce<broker::set>();
	announce<broker::table>();
	announce<broker::vector>();
	announce<expiration_time::tag>();
	announce<expiration_time>(&expiration_time::type, &expiration_time::time);
	announce<result::tag>();
	announce<result::status>();
	announce(typeid(result),
	         unique_ptr<caf::uniform_type_info>(new result_type_info));
	announce<query::tag>();
	announce<query>(&query::type, &query::k);
	announce<response>(&response::request, &response::reply, &response::cookie);
	announce<std::deque<response>>();
	announce<message>();
	announce<std::deque<message>>();
	announce<std::deque<peer_status>>();

	report::manager = std::unique_ptr<endpoint>(new endpoint("broker.reports"));

	if ( (flags & BROKER_INIT_DEFAULT_REPORT_QUEUE) )
		report::default_queue = std::unique_ptr<message_queue>(
	        new message_queue("broker.report.", *report::manager));

	BROKER_DEBUG("library", "successful initialization");
	return 0;
	}

int broker::init(int flags)
	{ return broker_init(flags); }

void broker_done()
	{
	broker::report::manager.reset();
	broker::report::default_queue.reset();
	caf::shutdown();
	}

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
