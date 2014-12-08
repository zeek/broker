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
#include <type_traits>
#include <cstdio>
#include <cstring>
#include <deque>
#include <mutex>

namespace broker { namespace report {
std::mutex* mtx;
}}

int broker_init(int flags)
	{
	// TODO: need a better, more organized way to announce types.
	using caf::announce;
	using namespace std;
	using namespace broker;
	using namespace broker::store;

	broker::report::mtx = new std::mutex{};
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
	announce<snapshot>(&snapshot::entries, &snapshot::sn);
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
	return 0;
	}

int broker::init(int flags)
	{ return broker_init(flags); }

void broker_done()
	{
	caf::shutdown();
	broker::report::done();
	delete broker::report::mtx;
	}

const char* broker_strerror(int broker_errno)
	{
	switch ( broker_errno ) {
	default:
		return ::strerror(broker_errno);
	}
	}

static void strerror_r_helper(char* result, char* buf, size_t buflen)
	{
	// Seems the GNU flavor of strerror_r may return a pointer to a static
	// string.  So try to copy as much as possible in to desire buffer.
	auto len = strlen(result);
	strncpy(buf, result, buflen);

	if ( len >= buflen )
		buf[buflen - 1] = 0;
	}

static void strerror_r_helper(int result, char* buf, size_t buflen)
	{ /* XSI flavor of strerror_r, no-op. */ }

void broker_strerror_r(int broker_errno, char* buf, size_t buflen)
	{
	switch ( broker_errno ) {
	default:
		{
		auto res = ::strerror_r(broker_errno, buf, buflen);
		// GNU vs. XSI flavors make it harder to use strerror_r.
		strerror_r_helper(res, buf, buflen);
		}
		break;
	}
	}
