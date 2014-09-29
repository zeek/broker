#ifndef BROKER_LOG_MSG_HH
#define BROKER_LOG_MSG_HH

#include <broker/data.hh>
#include <broker/util/operators.hh>
#include <string>

namespace broker {

/**
 * A message containing the information to be used for the purpose of
 * remote logging.
 */
struct log_msg : util::totally_ordered<log_msg> {

	/**
	  * Default constructor.
	  */
	log_msg() = default;

	/**
	  * Construct from a given stream and record.
	  */
	log_msg(std::string arg_stream, broker::record arg_fields)
		: stream(std::move(arg_stream)), fields(std::move(arg_fields))
		{}

	/**
	 * Name of a logging stream to write the message to on the receiving end.
	 */
	std::string stream;

	/**
	 * The contents of what is to be logged.
	 */
	broker::record fields;
};

inline bool operator==(const log_msg& lhs, const log_msg& rhs)
	{ return lhs.stream == rhs.stream && lhs.fields == rhs.fields; }

inline bool operator<(const log_msg& lhs, const log_msg& rhs)
	{ return lhs.stream < rhs.stream ||
	         ( ! (rhs.stream < lhs.stream) && lhs.fields < rhs.fields ); }

} // namespace broker

#endif // BROKER_LOG_MSG_HH
