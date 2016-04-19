#ifndef BROKER_ATOMS_HH
#define BROKER_ATOMS_HH

#include <caf/atom.hpp>

namespace broker {
namespace atom {

// Inherited from CAF.
using add = caf::add_atom;
using get = caf::get_atom;
using ok = caf::ok_atom;
using put = caf::put_atom;
using join = caf::join_atom;
using leave = caf::leave_atom;

// Generic
using subscribe = caf::atom_constant<caf::atom("subscribe")>;
using unsubscribe = caf::atom_constant<caf::atom("usubscribe")>;
using peer = caf::atom_constant<caf::atom("peer")>;
using unpeer = caf::atom_constant<caf::atom("unpeer")>;

} // namespace atom
} // namespace broker

#endif // BROKER_ATOMS_HH
