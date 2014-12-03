#include "broker/report.hh"
#include <sys/time.h>

static double now()
	{
	struct timeval tv;
	gettimeofday(&tv, 0);
	return tv.tv_sec + (tv.tv_usec / 1000000.0);
	}

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

void broker::report::send(level lvl, topic subtopic, std::string msg)
	{
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
