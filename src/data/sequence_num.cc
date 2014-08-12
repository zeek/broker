#include "broker/data/sequence_num.hh"

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

broker::data::sequence_num broker::data::sequence_num::next() const
	{
	sequence_num rval = *this;
	increase_sequence(rval.sequence);
	return rval;
	}

broker::data::sequence_num& broker::data::sequence_num::operator++()
	{
	increase_sequence(sequence);
	return *this;
	}

broker::data::sequence_num broker::data::sequence_num::operator++(int)
	{
	sequence_num tmp = *this;
	operator++();
	return tmp;
	}
