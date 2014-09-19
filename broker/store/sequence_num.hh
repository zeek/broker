#ifndef BROKER_STORE_SEQUENCE_NUM_HH
#define BROKER_STORE_SEQUENCE_NUM_HH

#include <vector>
#include <cstdint>

namespace broker { namespace store {

class sequence_num {
public:

	sequence_num(std::vector<uint64_t> s = {0})
		: sequence(std::move(s))
		{ }

    sequence_num next() const;

    sequence_num& operator++();

    sequence_num operator++(int);

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
