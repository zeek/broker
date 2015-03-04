#include "broker/broker.hh"
#include "broker/report.hh"
#include "broker/time_duration.hh"
#include "broker/time_point.hh"
#include "broker/store/backend.hh"
#include "broker/store/query.hh"
#include "broker/store/response.hh"
#include "broker/store/expiration_time.hh"
#include "store/result_type_info.hh"
#include "store/value_type_info.hh"
#include "data_type_info.hh"
#include "address_type_info.hh"
#include "subnet_type_info.hh"
#include "port_type_info.hh"
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

	broker::report::mtx = new std::mutex{};
	announce(typeid(broker::topic_set),
	         std::unique_ptr<caf::uniform_type_info>(
	             new broker::topic_set_type_info));
	announce<broker::outgoing_connection_status::tag>(
	            "broker::outgoing_connection_status::tag");
	announce<broker::outgoing_connection_status>(
	            "broker::outgoing_connection_status",
	            &broker::outgoing_connection_status::relation,
	            &broker::outgoing_connection_status::status,
	            &broker::outgoing_connection_status::peer_name);
	announce<broker::incoming_connection_status::tag>(
	            "broker::incoming_connection_status::tag");
	announce<broker::incoming_connection_status>(
	            "broker::incoming_connection_status",
	            &broker::incoming_connection_status::status,
	            &broker::incoming_connection_status::peer_name);
	announce(typeid(broker::peering),
	         std::unique_ptr<caf::uniform_type_info>(
	             new broker::peering_type_info));
	announce<broker::peering::impl>("broker::peering::impl",
	                                &broker::peering::impl::endpoint_actor,
	                                &broker::peering::impl::peer_actor,
	                                &broker::peering::impl::remote,
	                                &broker::peering::impl::remote_tuple);
	announce<broker::store::sequence_num>("broker::store::sequence_num",
	                                    &broker::store::sequence_num::sequence);
	announce<broker::store::snapshot>("broker::store::snapshot",
	                                  &broker::store::snapshot::entries,
	                                  &broker::store::snapshot::sn);
	announce<broker::data::tag>("broker::data::tag");
	announce(typeid(broker::data),
	         std::unique_ptr<caf::uniform_type_info>(
	             new broker::data_type_info));
	announce(typeid(broker::util::optional<broker::data>),
	         std::unique_ptr<caf::uniform_type_info>(
	             new broker::optional_data_type_info));
	announce(typeid(broker::address),
	         std::unique_ptr<caf::uniform_type_info>(
	             new broker::address_type_info));
	announce(typeid(broker::subnet),
	         std::unique_ptr<caf::uniform_type_info>(
	             new broker::subnet_type_info));
	announce(typeid(broker::port),
	         std::unique_ptr<caf::uniform_type_info>(
	             new broker::port_type_info));
	announce<broker::time_duration>("broker::time_duration",
	                                &broker::time_duration::value);
	announce<broker::time_point>("broker::time_point",
	                             &broker::time_point::value);
	announce<broker::enum_value>("broker::enum_value",
	                             &broker::enum_value::name);
	announce<broker::record>("broker::record", &broker::record::fields);
	announce<std::unordered_set<broker::data>>(
	            "std::unordered_set<broker::data>");
	announce<broker::set>("broker::set");
	announce<broker::table>("broker::table");
	announce<broker::vector>("broker::vector");
	announce<broker::store::expiration_time::tag>(
	            "broker::store::expiration_time::tag");
	announce<broker::store::expiration_time>("broker::store::expiration_time",
	                        &broker::store::expiration_time::expiry_time,
	                        &broker::store::expiration_time::modification_time,
	                        &broker::store::expiration_time::type);
	announce(typeid(broker::store::value),
	         std::unique_ptr<caf::uniform_type_info>(
	             new broker::store::value_type_info));
	announce<broker::store::result::tag>("broker::store::result::tag");
	announce<broker::store::result::status>("broker::store::result::status");
	announce(typeid(broker::store::result),
	         std::unique_ptr<caf::uniform_type_info>(
	             new broker::store::result_type_info));
	announce<broker::store::query::tag>("broker::store::query::tag");
	announce<broker::store::query>("broker::store::query",
	                               &broker::store::query::type,
	                               &broker::store::query::k);
	announce<broker::store::response>("broker::store::response",
	                                  &broker::store::response::request,
	                                  &broker::store::response::reply,
	                                  &broker::store::response::cookie);
	announce<std::deque<broker::store::response>>(
	            "std::deque<broker::store::response>");
	announce<broker::message>("broker::message");
	announce<std::deque<broker::message>>("std::deque<broker::message>");
	announce<std::deque<broker::outgoing_connection_status>>(
	            "std::deque<broker::outgoing_connection_status>");
	announce<std::deque<broker::incoming_connection_status>>(
	            "std::deque<broker::incoming_connection_status>");
	return 0;
	}

int broker::init(int flags)
	{ return broker_init(flags); }

void broker::done()
	{ return broker_done(); }

void broker_done()
	{
	caf::shutdown();
	broker::report::done();
	delete broker::report::mtx;
	}

const char* broker::strerror(int broker_errno)
	{ return broker_strerror(broker_errno); }

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

void broker::strerror_r(int broker_errno, char* buf, size_t buflen)
	{ return broker_strerror_r(broker_errno, buf, buflen); }

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
