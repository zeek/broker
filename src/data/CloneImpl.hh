#ifndef BROKER_DATA_CLONEIMPL_HH
#define BROKER_DATA_CLONEIMPL_HH

#include "broker/data/Clone.hh"

#include <cppa/cppa.hpp>

namespace broker { namespace data {

class Clone::Impl {
public:

	cppa::actor clone;
};

} // namespace data
} // namespace broker

#endif // BROKER_DATA_CLONEIMPL_HH
