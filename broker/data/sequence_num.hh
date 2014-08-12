#ifndef BROKER_DATA_SEQUENCE_NUM_HH
#define BROKER_DATA_SEQUENCE_NUM_HH

#include <vector>
#include <cstdint>

namespace broker { namespace data {

class sequence_num {
public:

    sequence_num next() const;

    sequence_num& operator++();

    sequence_num operator++(int);

    std::vector<uint64_t> sequence = { 0 };
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

} // namespace data
} // namespace broker

#endif // BROKER_DATA_SEQUENCE_NUM_HH
