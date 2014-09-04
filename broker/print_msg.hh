#ifndef BROKER_PRINT_MSG_HH
#define BROKER_PRINT_MSG_HH

#include <string>

namespace broker {

/**
 * A message containing the information to be used for the purpose of
 * remote printing.
 */
struct print_msg {

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

inline bool operator!=(const print_msg& lhs, const print_msg& rhs)
	{ return ! operator==(lhs,rhs); }

inline bool operator<(const print_msg& lhs, const print_msg& rhs)
	{ return lhs.path < rhs.path ||
	         ( ! (rhs.path < lhs.path) && lhs.data < rhs.data ); }

inline bool operator>(const print_msg& lhs, const print_msg& rhs)
	{ return operator<(rhs,lhs); }

inline bool operator<=(const print_msg& lhs, const print_msg& rhs)
	{ return ! operator>(lhs,rhs); }

inline bool operator>=(const print_msg& lhs, const print_msg& rhs)
	{ return ! operator<(lhs,rhs); }

} // namespace broker

#endif // BROKER_PRINT_MSG_HH
