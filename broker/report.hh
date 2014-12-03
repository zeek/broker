#ifndef BROKER_REPORT_HH
#define BROKER_REPORT_HH

#include <broker/endpoint.hh>
#include <broker/message_queue.hh>
#include <type_traits>
#include <memory>
#include <string>

namespace broker {
namespace report {

/**
 * An endpoint to which broker library diagnostic messages are sent.
 * This is an endpoint like any other broker endpoint, reports sent to it
 * use a topic prefixed with "broker.report.<level>." where "<level>" is
 * "debug", "info", "warn", or "error".  The endpoint is created by calling
 * broker::init().
 */
extern std::unique_ptr<endpoint> manager;

/**
 * A default queue to which broker library diagnostic messages accumulate.
 * The queue is optionally created by calling broker::init() with
 * BROKER_INIT_DEFAULT_REPORT_QUEUE flag set. The application should either
 * drain this queue periodically or, if it no longer wants to process
 * messages it can call reset() on it at anytime. Messages are a sequence of:
 * [timestamp (real), level (count), subtopic (string), contents (string)].
 */
extern std::unique_ptr<message_queue> default_queue;

/**
 * A level indicating the criticality of an associated message.
 * debug - verbose diagnostics (these are preprocessed out unless compiling
 *         broker in debug mode).
 * info - informational (library is more likely to use "debug" than this).
 * warn - the library may still operate correctly, but temporarily could not
 *        complete some function (e.g. transient network issues, system outage).
 * error - the library is not operating correctly and needs manual intervention.
 */
enum class level : uint8_t {
	debug,
	info,
	warn,
	error,
};

/**
 * @return integral type of the report level.
 */
constexpr std::underlying_type<level>::type operator+(level v)
	{ return static_cast<std::underlying_type<level>::type>(v); }

/**
 * Send a report message.
 * @param lvl criticality of the message.
 * @param subtopic i.e. "broker.report.<lvl>.<subtopic>"
 * @param msg The information to report.
 */
void send(level lvl, topic subtopic, std::string msg);

/**
 * @see broker::report::send() with a level of broker::report::level::info.
 */
void info(topic subtopic, std::string msg);

/**
 * @see broker::report::send() with a level of broker::report::level::warn.
 */
void warn(topic subtopic, std::string msg);

/**
 * @see broker::report::send() with a level of broker::report::level::error.
 */
void error(topic subtopic, std::string msg);

#ifdef DEBUG
/**
 * @see broker::report::send() with a level of broker::report::level::debug.
 */
#define BROKER_DEBUG(subtopic, msg) \
	broker::report::send(broker::report::level::debug, subtopic, msg)
#else
#define BROKER_DEBUG(subtopic, msg)
#endif

} // namespace report
} // namespace broker

#endif // BROKER_REPORT_HH
