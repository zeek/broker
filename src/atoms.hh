#ifndef BROKER_ATOMS_HH
#define BROKER_ATOMS_HH

#include <caf/atom.hpp>

namespace broker {

using want_atom = caf::atom_constant<caf::atom("want")>;
using need_atom = caf::atom_constant<caf::atom("need")>;
using flags_atom = caf::atom_constant<caf::atom("flags")>;
using peerstat_atom = caf::atom_constant<caf::atom("peerstat")>;
using peer_atom = caf::atom_constant<caf::atom("peer")>;
using unpeer_atom = caf::atom_constant<caf::atom("unpeer")>;
using quit_atom = caf::atom_constant<caf::atom("quit")>;
using acl_pub_atom = caf::atom_constant<caf::atom("acl pub")>;
using acl_unpub_atom = caf::atom_constant<caf::atom("acl unpub")>;
using advert_atom = caf::atom_constant<caf::atom("advert")>;
using unadvert_atom = caf::atom_constant<caf::atom("unadvert")>;
using sub_atom = caf::atom_constant<caf::atom("sub")>;
using local_sub_atom = caf::atom_constant<caf::atom("local sub")>;
using unsub_atom = caf::atom_constant<caf::atom("unsub")>;
using master_atom = caf::atom_constant<caf::atom("master")>;
using store_actor_atom = caf::atom_constant<caf::atom("storeactor")>;

namespace store {

using increment_atom = caf::atom_constant<caf::atom("increment")>;
using set_add_atom = caf::atom_constant<caf::atom("set_add")>;
using set_rem_atom = caf::atom_constant<caf::atom("set_rem")>;
using lpush_atom = caf::atom_constant<caf::atom("lpush")>;
using rpush_atom = caf::atom_constant<caf::atom("rpush")>;
using lpop_atom = caf::atom_constant<caf::atom("lpop")>;
using rpop_atom = caf::atom_constant<caf::atom("rpop")>;
using insert_atom = caf::atom_constant<caf::atom("insert")>;
using erase_atom = caf::atom_constant<caf::atom("erase")>;
using expire_atom = caf::atom_constant<caf::atom("expire")>;
using clear_atom = caf::atom_constant<caf::atom("clear")>;
using find_master_atom = caf::atom_constant<caf::atom("findmaster")>;
using get_snap_atom = caf::atom_constant<caf::atom("getsnap")>;

} // namespace store
} // namespace broker

#endif // BROKER_ATOMS_HH
