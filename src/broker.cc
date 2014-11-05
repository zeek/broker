#include "broker/broker.hh"
#include "broker/peer_status.hh"
#include "broker/message.hh"
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
	announce<record>(&record::fields);
	announce(typeid(util::optional<data>),
	         unique_ptr<caf::uniform_type_info>(new optional_data_type_info));
	announce(typeid(data),
	         unique_ptr<caf::uniform_type_info>(new data_type_info));
	announce<std::unordered_set<data>>();
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
