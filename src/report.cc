#include "broker/report.hh"
#include "broker/time_point.hh"
#include <mutex>

namespace broker { namespace report {
extern std::mutex* mtx;
endpoint* manager;
message_queue* default_queue;
}}

static inline double now()
	{ return broker::time_point::now().value; }

static std::string to_string(broker::report::level lvl)
	{
	using broker::report::level;

	switch ( lvl ) {
	case level::debug:
		return "debug";
	case level::info:
		return "info";
	case level::warn:
		return "warn";
	case level::error:
		return "error";
	default:
		return "<unknown>";
	}
	}

int broker::report::init(bool with_default_queue)
	{
	std::unique_lock<std::mutex> guard{*mtx};

	manager = new endpoint("broker.reports");

	if ( with_default_queue )
		default_queue = new message_queue("broker.report.", *manager);

	return 0;
	}

void broker::report::done()
	{
	std::unique_lock<std::mutex> guard{*mtx};
	delete manager;
	manager = nullptr;
	delete default_queue;
	default_queue = nullptr;
	}

void broker::report::send(level lvl, topic subtopic, std::string msg)
	{
	std::unique_lock<std::mutex> guard{*mtx};

	if ( ! manager )
		return;

	topic full_topic = "broker.report." + ::to_string(lvl) + "." + subtopic;
	message full_message{now(), +lvl, std::move(subtopic), std::move(msg)};
	manager->send(std::move(full_topic), std::move(full_message));
	}

void broker::report::info(topic subtopic, std::string msg)
	{ send(level::info, std::move(subtopic), std::move(msg)); }

void broker::report::warn(topic subtopic, std::string msg)
	{ send(level::warn, std::move(subtopic), std::move(msg)); }

void broker::report::error(topic subtopic, std::string msg)
	{ send(level::error, std::move(subtopic), std::move(msg)); }
