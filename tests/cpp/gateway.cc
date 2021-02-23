#define SUITE gateway

#include "broker/gateway.hh"

#include "test.hh"

#include "broker/alm/peer.hh"
#include "broker/alm/stream_transport.hh"

// TODO: implement me

// using namespace broker;
//
// namespace {
//
// // -- actor type: peer with stream transport -----------------------------------
//
// using peer_id = endpoint_id;
//
// class peer_manager : public alm::stream_transport {
// public:
//   using super = alm::stream_transport;
//
//   peer_manager(caf::event_based_actor* self) : super(self) {
//     // nop
//   }
//
//   auto hdl() noexcept {
//     return caf::actor_cast<caf::actor>(self());
//   }
//
//   using super::ship_locally;
//
//   void ship_locally(const data_message& msg) {
//     buf.emplace_back(msg);
//     super::ship_locally(msg);
//   }
//
//   std::vector<peer_id> shortest_path(const peer_id& to) {
//     if (auto ptr = alm::shortest_path(tbl(), to))
//       return *ptr;
//     return {};
//   }
//
//   std::vector<data_message> buf;
// };
//
// struct peer_actor_state {
//   caf::intrusive_ptr<peer_manager> mgr;
//   static inline const char* name = "peer";
// };
//
// using peer_actor_type = caf::stateful_actor<peer_actor_state>;
//
// caf::behavior peer_actor(peer_actor_type* self, peer_id id) {
//   auto& mgr = self->state.mgr;
//   mgr = caf::make_counted<peer_manager>(self);
//   mgr->id(std::move(id));
//   return mgr->make_behavior();
// }
//
// // -- fixture ------------------------------------------------------------------
//
//
// // This fixture configures the following topology:
// //
// //
// //               internal                            external
// //      <------------------------->         <------------------------->
// //
// //
// //                      +---+                     +---+           +---+
// //                +-----+ D +---+             +---+ H +-----------+ K |
// //                |     +---+   |             |   +---+           +---+
// //              +-+-+           |             |
// //        +-----+ B |           |  +-------+  |
// //        |     +---+           +--+       +--+
// //      +-+-+           +---+      |       |      +---+
// //      | A |           | E +------+   G   +------+ I |
// //      +-+-+           +---+      |       |      +---+
// //        |     +---+           +--+       +--+
// //        +-----+ C |           |  +-------+  |
// //              +-+-+           |             |
// //                |     +---+   |             |   +---+           +---+
// //                +-----+ F +---+             +---+ J +-----------+ L |
// //                      +---+                     +---+           +---+
// //
//
// #define PEER_ID(var, num) peer_id var = make_peer_id(num)
// #define PEER_EXPAND(var) std::make_pair(std::string{#var}, var)
//
// struct fixture : test_coordinator_fixture<> {
//   static endpoint_id make_peer_id(uint8_t num) {
//     std::array<uint8_t, 20> host_id;
//     host_id.fill(num);
//     return caf::make_node_id(num, host_id);
//   }
//
//   PEER_ID(A, 1);
//   PEER_ID(B, 2);
//   PEER_ID(C, 3);
//   PEER_ID(D, 4);
//   PEER_ID(E, 5);
//   PEER_ID(F, 6);
//   PEER_ID(G, 7);
//   PEER_ID(H, 8);
//   PEER_ID(I, 9);
//   PEER_ID(J, 10);
//   PEER_ID(K, 11);
//   PEER_ID(L, 12);
//
//   fixture() {
//     peers["internal"] = sys.spawn(peer_actor, G);
//     peers["external"] = sys.spawn(peer_actor, G);
//     gateway::setup(peers["internal"], peers["external"]);
//     // Note: skips G on purpose. This ID is used by `internal` and `external`.
//     std::vector<std::pair<std::string, endpoint_id>> cfg{
//       PEER_EXPAND(A), PEER_EXPAND(B), PEER_EXPAND(C), PEER_EXPAND(D),
//       PEER_EXPAND(E), PEER_EXPAND(F), PEER_EXPAND(H), PEER_EXPAND(I),
//       PEER_EXPAND(J), PEER_EXPAND(K), PEER_EXPAND(L),
//     };
//     for (const auto& [name, id] : cfg) {
//       names[id] = name;
//       peers[name] = sys.spawn(peer_actor, id);
//     }
//     run();
//   }
//
//   ~fixture() {
//     for (auto& kvp : peers)
//       anon_send_exit(kvp.second, caf::exit_reason::kill);
//   }
//
//   auto& get(const caf::actor& hdl) {
//     return *deref<peer_actor_type>(hdl).state.mgr;
//   }
//
//   auto& get(const endpoint_id& id) {
//     return get(peers[names[id]]);
//   }
//
//   template <class Fun>
//   void for_each_peer(Fun fun) {
//     for (const auto& id : {A, B, C, D, E, F, H, I, J, K, L})
//       fun(get(id));
//     run();
//   }
//
//   void connect_peers() {
//     std::map<std::string, std::vector<std::string>> connections{
//       {"A", {"B", "C"}},
//       {"B", {"A", "D"}},
//       {"C", {"A", "F"}},
//       {"D", {"B", "internal"}},
//       {"E", {"internal"}},
//       {"F", {"C", "internal"}},
//       {"H", {"K", "external"}},
//       {"I", {"external"}},
//       {"J", {"L", "external"}},
//       {"K", {"H"}},
//       {"L", {"J"}},
//     };
//     auto link_id = [this](const std::string& name) {
//       return get(peers[name]).id();
//     };
//     for (auto& [id, links] : connections)
//       for (auto& link : links)
//         anon_send(peers[id], atom::peer_v, link_id(link), peers[link]);
//     run();
//   }
//
//   std::map<endpoint_id, std::string> names;
//   std::map<std::string, caf::actor> peers;
// };
//
// } // namespace
//
// #define CHECK_DISTANCE(src, dst, val)                                          \
//   CHECK_EQUAL(alm::distance_to(get(src).tbl(), dst), optional<size_t>{val})
//
// FIXTURE_SCOPE(gateway_tests, fixture)
//
// TEST(gateways separate internal and external domain) {
//   connect_peers();
//   MESSAGE("peer A only sees peers from the internal domain");
//   CHECK_DISTANCE(A, B, 1);
//   CHECK_DISTANCE(A, C, 1);
//   CHECK_DISTANCE(A, D, 2);
//   CHECK_DISTANCE(A, E, 4);
//   CHECK_DISTANCE(A, F, 2);
//   CHECK_DISTANCE(A, G, 3);
//   CHECK_DISTANCE(A, H, nil);
//   CHECK_DISTANCE(A, I, nil);
//   CHECK_DISTANCE(A, J, nil);
//   CHECK_DISTANCE(A, K, nil);
//   CHECK_DISTANCE(A, L, nil);
//   MESSAGE("peer I only sees peers from the external domain");
//   CHECK_DISTANCE(I, A, nil);
//   CHECK_DISTANCE(I, B, nil);
//   CHECK_DISTANCE(I, C, nil);
//   CHECK_DISTANCE(I, D, nil);
//   CHECK_DISTANCE(I, E, nil);
//   CHECK_DISTANCE(I, F, nil);
//   CHECK_DISTANCE(I, G, 1);
//   CHECK_DISTANCE(I, H, 2);
//   CHECK_DISTANCE(I, J, 2);
//   CHECK_DISTANCE(I, K, 3);
//   CHECK_DISTANCE(I, L, 3);
// }
//
// TEST(gateways forward messages between the domains) {
//   for_each_peer([](auto& state) { state.subscribe({"foo", "bar"}); });
//   connect_peers();
//   MESSAGE("publish to 'foo' on A");
//   anon_send(peers["A"], atom::publish_v, make_data_message("foo", 42));
//   run();
//   MESSAGE("publish to 'bar' on I");
//   anon_send(peers["I"], atom::publish_v, make_data_message("bar", 23));
//   run();
//   MESSAGE("all peers must have received messages from both domains");
//   using log_t = std::vector<data_message>;
//   log_t log{make_data_message("foo", 42), make_data_message("bar", 23)};
//   CHECK_EQUAL(get(A).buf, log_t{make_data_message("bar", 23)});
//   CHECK_EQUAL(get(B).buf, log);
//   CHECK_EQUAL(get(C).buf, log);
//   CHECK_EQUAL(get(D).buf, log);
//   CHECK_EQUAL(get(E).buf, log);
//   CHECK_EQUAL(get(F).buf, log);
//   CHECK_EQUAL(get(H).buf, log);
//   CHECK_EQUAL(get(I).buf, log_t{make_data_message("foo", 42)});
//   CHECK_EQUAL(get(J).buf, log);
//   CHECK_EQUAL(get(K).buf, log);
//   CHECK_EQUAL(get(L).buf, log);
// }
//
// FIXTURE_SCOPE_END()

TEST(todo) {
  MESSAGE("implement me");
}
