#ifndef BROKER_DATA_MASTERIMPL_HH
#define BROKER_DATA_MASTERIMPL_HH

#include "broker/data/Master.hh"

#include <cppa/cppa.hpp>

namespace broker { namespace data {

class Master::Impl {
public:

	cppa::actor master;
};

} // namespace data
} // namespace broker

#endif // BROKER_DATA_MASTERIMPL_HH
