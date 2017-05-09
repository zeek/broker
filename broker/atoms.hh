#ifndef BROKER_ATOMS_HH
#define BROKER_ATOMS_HH

#include <caf/atom.hpp>

namespace broker {
namespace atom {

/// --- inherited from CAF -----------------------------------------------------

using add = caf::add_atom;
using get = caf::get_atom;
using join = caf::join_atom;
using leave = caf::leave_atom;
using ok = caf::ok_atom;
using put = caf::put_atom;
using connect = caf::connect_atom;
using subscribe = caf::subscribe_atom;
using unsubscribe = caf::unsubscribe_atom;
using tick = caf::tick_atom;
using publish = caf::publish_atom;

/// --- generic communication --------------------------------------------------

using name = caf::atom_constant<caf::atom("name")>;
using network = caf::atom_constant<caf::atom("network")>;
using peer = caf::atom_constant<caf::atom("peer")>;
using status = caf::atom_constant<caf::atom("status")>;
using unpeer = caf::atom_constant<caf::atom("unpeer")>;
using default_ = caf::atom_constant<caf::atom("default")>;

/// --- communiation with workers ----------------------------------------------

using resume = caf::atom_constant<caf::atom("resume")>;

/// --- communiation with stores -----------------------------------------------

using attach = caf::atom_constant<caf::atom("attach")>;
using clear = caf::atom_constant<caf::atom("clear")>;
using clone = caf::atom_constant<caf::atom("clone")>;
using decrement = caf::atom_constant<caf::atom("decrement")>;
using erase = caf::atom_constant<caf::atom("erase")>;
using expire = caf::atom_constant<caf::atom("expire")>;
using increment = caf::atom_constant<caf::atom("increment")>;
using keys = caf::atom_constant<caf::atom("keys")>;
using master = caf::atom_constant<caf::atom("master")>;
using snapshot = caf::atom_constant<caf::atom("snapshot")>;
using store = caf::atom_constant<caf::atom("store")>;
using subtract = caf::atom_constant<caf::atom("subtract")>;

} // namespace atom
} // namespace broker

#endif // BROKER_ATOMS_HH
