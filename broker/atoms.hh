#ifndef BROKER_ATOMS_HH
#define BROKER_ATOMS_HH

#include <caf/atom.hpp>

namespace broker {
namespace atom {

// Inherited from CAF.
using add = caf::add_atom;
using get = caf::get_atom;
using join = caf::join_atom;
using leave = caf::leave_atom;
using ok = caf::ok_atom;
using put = caf::put_atom;

// Generic
using connect = caf::atom_constant<caf::atom("connect")>;
using network = caf::atom_constant<caf::atom("network")>;
using peer = caf::atom_constant<caf::atom("peer")>;
using subscribe = caf::atom_constant<caf::atom("subscribe")>;
using unpeer = caf::atom_constant<caf::atom("unpeer")>;
using unsubscribe = caf::atom_constant<caf::atom("usubscribe")>;

// Store
using attach = caf::atom_constant<caf::atom("attach")>;
using clone = caf::atom_constant<caf::atom("clone")>;
using erase = caf::atom_constant<caf::atom("erase")>;
using expire = caf::atom_constant<caf::atom("expire")>;
using increment = caf::atom_constant<caf::atom("increment")>;
using decrement = caf::atom_constant<caf::atom("decrement")>;
using master = caf::atom_constant<caf::atom("master")>;
using remove = caf::atom_constant<caf::atom("remove")>;
using snapshot = caf::atom_constant<caf::atom("snapshot")>;
using store = caf::atom_constant<caf::atom("store")>;

} // namespace atom
} // namespace broker

#endif // BROKER_ATOMS_HH
