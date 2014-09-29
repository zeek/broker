#ifndef BROKER_EVENT_MSG_HH
#define BROKER_EVENT_MSG_HH

#include <broker/data.hh>
#include <broker/util/operators.hh>
#include <string>

namespace broker {

/**
 * A message containing the information to be used for the purpose of
 * remote events.
 */
struct event_msg : util::totally_ordered<event_msg> {

	/**
	  * Default constructor.
	  */
	event_msg() = default;

	/**
	 * Construct from given name and arguments.
	 */
	event_msg(std::string arg_name, broker::vector arg_args)
		: name(std::move(arg_name)), args(std::move(arg_args))
		{}

	/**
	 * A name/identifier for the event.
	 */
	std::string name;

	/**
	 * The data/arguments associated with the event.
	 */
	broker::vector args;
};

inline bool operator==(const event_msg& lhs, const event_msg& rhs)
	{ return lhs.name == rhs.name && lhs.args == rhs.args; }

inline bool operator<(const event_msg& lhs, const event_msg& rhs)
	{ return lhs.name < rhs.name ||
	         ( ! (rhs.name < lhs.name) && lhs.args < rhs.args ); }

} // namespace broker

#endif // BROKER_EVENT_MSG_HH
