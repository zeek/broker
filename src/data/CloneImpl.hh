#ifndef BROKER_DATA_CLONEIMPL_HH
#define BROKER_DATA_CLONEIMPL_HH

#include "broker/data/Clone.hh"

#include <caf/actor.hpp>

namespace broker { namespace data {

class Clone::Impl {
public:

	caf::actor clone;
};

} // namespace data
} // namespace broker

#endif // BROKER_DATA_CLONEIMPL_HH
