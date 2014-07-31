#ifndef BROKER_DATA_MASTERIMPL_HH
#define BROKER_DATA_MASTERIMPL_HH

#include "broker/data/Master.hh"

#include <caf/actor.hpp>

namespace broker { namespace data {

class Master::Impl {
public:

	caf::actor master;
};

} // namespace data
} // namespace broker

#endif // BROKER_DATA_MASTERIMPL_HH
