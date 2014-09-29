#ifndef BROKER_PRINT_MSG_HH
#define BROKER_PRINT_MSG_HH

#include <broker/util/operators.hh>
#include <string>

namespace broker {

/**
 * A message containing the information to be used for the purpose of
 * remote printing.
 */
struct print_msg : util::totally_ordered<print_msg> {

	/**
	  * Default constructor.
	  */
	print_msg() = default;

	/**
	 * Construct from given path and data strings.
	 */
	print_msg(std::string arg_path, std::string arg_data)
		: path(std::move(arg_path)), data(std::move(arg_data))
		{}

	/**
	 * A logical path to write the message to on the receiving end.
	 * Interpretation is left to the receiver.
	 */
	std::string path;

	/**
	 * The contents of what is to be printed.
	 */
	std::string data;
};

inline bool operator==(const print_msg& lhs, const print_msg& rhs)
	{ return lhs.path == rhs.path && lhs.data == rhs.data; }

inline bool operator<(const print_msg& lhs, const print_msg& rhs)
	{ return lhs.path < rhs.path ||
	         ( ! (rhs.path < lhs.path) && lhs.data < rhs.data ); }

} // namespace broker

#endif // BROKER_PRINT_MSG_HH
