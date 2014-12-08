#ifndef BROKER_STORE_SEQUENCE_NUM_HH
#define BROKER_STORE_SEQUENCE_NUM_HH

#include <vector>
#include <cstdint>

namespace broker { namespace store {

/**
 * A number that may be incremented by one without limit (well, technically
 * constrained by memory, but you will be counting for a long time before
 * reaching that limit).
 */
class sequence_num {
public:

	/**
	 * Construct a sequence number initialized to zero.
	 */
	sequence_num()
		: sequence({0})
		{}

	/**
	 * Construct a sequence number, starting at a given number.
	 */
	sequence_num(std::vector<uint64_t> s)
		: sequence(std::move(s))
		{ if ( sequence.empty() ) sequence = {0};  }

	/**
	 * @return the next number in the sequence.
	 */
    sequence_num next() const;

	/**
	 * Pre-increment the sequence number.
	 * @return reference of the sequence number after incrementing.
	 */
    sequence_num& operator++();

	/**
	 * Post-increment the sequence number.
	 * @return copy of the sequence number before incrementing.
	 */
    sequence_num operator++(int);

	// Stored with most-significant part at index 0.
    std::vector<uint64_t> sequence;
};

inline bool operator==(const sequence_num& lhs, const sequence_num& rhs)
    { return lhs.sequence == rhs.sequence; }
inline bool operator!=(const sequence_num& lhs, const sequence_num& rhs)
    { return ! operator==(lhs,rhs); }
inline bool operator<(const sequence_num& lhs, const sequence_num& rhs)
    {
    if ( lhs.sequence.size() == rhs.sequence.size() )
        return lhs.sequence < rhs.sequence;
    return lhs.sequence.size() < rhs.sequence.size();
    }
inline bool operator>(const sequence_num& lhs, const sequence_num& rhs)
    { return operator<(rhs,lhs); }
inline bool operator<=(const sequence_num& lhs, const sequence_num& rhs)
    { return ! operator>(lhs,rhs); }
inline bool operator>=(const sequence_num& lhs, const sequence_num& rhs)
    { return ! operator<(lhs,rhs); }

} // namespace store
} // namespace broker

#endif // BROKER_STORE_SEQUENCE_NUM_HH
