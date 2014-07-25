#include "broker/data/SequenceNum.hh"

static void increase_sequence(std::vector<uint64_t>& s)
	{
    for ( int i = s.size() - 1; i >= 0; --i )
        {
        ++s[i];

        if ( s[i] != 0 )
            break;

        if ( i == 0 )
            s.insert(s.begin(), 1);
        }
	}

broker::data::SequenceNum broker::data::SequenceNum::Next() const
	{
	SequenceNum rval = *this;
	increase_sequence(rval.sequence);
	return rval;
	}

broker::data::SequenceNum& broker::data::SequenceNum::operator++()
	{
	increase_sequence(sequence);
	return *this;
	}

broker::data::SequenceNum broker::data::SequenceNum::operator++(int)
	{
	SequenceNum tmp = *this;
	operator++();
	return tmp;
	}
