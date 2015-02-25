#ifndef BROKER_PERSISTABLES_HH
#define BROKER_PERSISTABLES_HH

#include "util/persist.hh"
#include "broker/data.hh"
#include "broker/store/expiration_time.hh"

namespace broker {

void save(util::persist::save_archive& ar, const data& d, uint32_t version);
void load(util::persist::load_archive& ar, data& d, uint32_t version);

namespace store {

void save(util::persist::save_archive& ar, const expiration_time& d,
          uint32_t version);
void load(util::persist::load_archive& ar, expiration_time& d,
          uint32_t version);

} // namespace store
} // namespace broker

#endif // BROKER_PERSISTABLES_HH
