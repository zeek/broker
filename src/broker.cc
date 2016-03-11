#include <type_traits>
#include <cstdio>
#include <cstring>
#include <deque>
#include <mutex>

#include <caf/actor_system.hpp>
#include <caf/actor_system_config.hpp>
#include <caf/io/middleman.hpp>

#include "broker/broker.hh"
#include "broker/report.hh"
#include "broker/time_duration.hh"
#include "broker/time_point.hh"
#include "broker/store/backend.hh"
#include "broker/store/query.hh"
#include "broker/store/response.hh"
#include "broker/store/expiration_time.hh"
#include "broker/util/make_unique.hh"

#include "peering_impl.hh"
#include "queue_impl.hh"
#include "subscription.hh"

namespace broker {

// TODO: This global state has to go with broker systems.
namespace report {
std::mutex* mtx;
} // namespace report

// The global actor system.
// TODO: We only use one actor system during the migration to CAF 0.15. Later,
// this global state will go away and broker_init will return a broker system
// instead.
std::unique_ptr<caf::actor_system> broker_system;

} // namespace broker


int broker_init(int flags)
	{
	broker::report::mtx = new std::mutex{};
  caf::actor_system_config cfg;
  cfg.load<caf::io::middleman>()
     .add_message_type<broker::topic_set>("broker::topic_set")
     .add_message_type<broker::outgoing_connection_status>(
       "broker::outgoing_connection_status")
     .add_message_type<broker::incoming_connection_status>(
       "broker::incoming_connection_status")
     .add_message_type<broker::peering>("broker::peering")
     .add_message_type<broker::peering::impl>("broker::peering_impl")
     .add_message_type<broker::store::sequence_num>(
       "broker::store::sequence_num")
     .add_message_type<broker::data>("broker::data")
     .add_message_type<broker::address>("broker::address")
     .add_message_type<broker::subnet>("broker::subnet")
     .add_message_type<broker::port>("broker::port")
     .add_message_type<broker::time_duration>("broker::time_duration")
     .add_message_type<broker::time_point>("broker::time_point")
     .add_message_type<broker::enum_value>("broker::enum_value")
     .add_message_type<broker::vector>("broker::vector")
     .add_message_type<broker::set>("broker::set")
     .add_message_type<broker::table>("broker::table")
     .add_message_type<broker::record>("broker::record")
     .add_message_type<broker::message>("broker::message")
     .add_message_type<broker::store::expiration_time>(
       "broker::store::expiration_time")
     .add_message_type<broker::store::query>("broker::store::query")
     .add_message_type<broker::store::response>("broker::store::response")
     .add_message_type<broker::store::result>("broker::store::result")
     .add_message_type<broker::store::snapshot>("broker::store::snapshot")
     .add_message_type<broker::store::value>("broker::store::value")
     .add_message_type<broker::store::value>("broker::store::value")
     .add_message_type<std::deque<broker::outgoing_connection_status>>(
       "std::deque<broker::outgoing_connection_status>")
     .add_message_type<std::deque<broker::incoming_connection_status>>(
       "std::deque<broker::incoming_connection_status>")
     .add_message_type<std::deque<broker::message>>(
       "std::deque<broker::message>")
     .add_message_type<std::deque<broker::store::response>>(
       "std::deque<broker::store::response>")
     .add_message_type<std::unordered_set<broker::data>>(
       "std::unordered_set<broker::data>")
	    ;
  broker::broker_system = std::make_unique<caf::actor_system>(std::move(cfg));
	return 0;
	}

int broker::init(int flags)
	{ return broker_init(flags); }

void broker::done()
	{ return broker_done(); }

void broker_done()
	{
	broker::report::done();
  broker::broker_system->await_actors_before_shutdown(false);
	broker::broker_system.reset();
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
