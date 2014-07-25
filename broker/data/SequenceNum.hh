#ifndef BROKER_DATA_SEQUENCENUM_HH
#define BROKER_DATA_SEQUENCENUM_HH

#include <vector>
#include <cstdint>

namespace broker { namespace data {

class SequenceNum {
public:

    SequenceNum Next() const;

    SequenceNum& operator++();

    SequenceNum operator++(int);

    std::vector<uint64_t> sequence = { 0 };
};

inline bool operator==(const SequenceNum& lhs, const SequenceNum& rhs)
    { return lhs.sequence == rhs.sequence; }
inline bool operator!=(const SequenceNum& lhs, const SequenceNum& rhs)
    { return ! operator==(lhs,rhs); }
inline bool operator<(const SequenceNum& lhs, const SequenceNum& rhs)
    {
    if ( lhs.sequence.size() == rhs.sequence.size() )
        return lhs.sequence < rhs.sequence;
    return lhs.sequence.size() < rhs.sequence.size();
    }
inline bool operator>(const SequenceNum& lhs, const SequenceNum& rhs)
    { return operator<(rhs,lhs); }
inline bool operator<=(const SequenceNum& lhs, const SequenceNum& rhs)
    { return ! operator>(lhs,rhs); }
inline bool operator>=(const SequenceNum& lhs, const SequenceNum& rhs)
    { return ! operator<(lhs,rhs); }

} // namespace data
} // namespace broker

#endif // BROKER_DATA_SEQUENCENUM_HH
