#define SUITE core_actor

#include "broker/core_actor.hh"

#include "test.hh"

#include "caf/attach_stream_sink.hpp"
#include "caf/attach_stream_source.hpp"

#include "broker/configuration.hh"
#include "broker/endpoint.hh"
#include "broker/logger.hh"

using caf::actor;
using caf::node_id;

// TODO: implement me

// using namespace broker;
// using namespace broker::detail;
//
// using element_type = endpoint::stream_type::value_type;
//
// namespace {
//
// struct driver_state {
//   using buf_type = std::vector<element_type>;
//   bool restartable = false;
//   buf_type xs;
//   static inline const char* name = "driver";
//   void reset() {
//     xs = data_msgs({{"a", 0}, {"b", true}, {"a", 1}, {"a", 2}, {"b", false},
//                     {"b", true}, {"a", 3}, {"b", false}, {"a", 4}, {"a", 5}});
//   }
//   driver_state() {
//     reset();
//   }
// };
//
// using driver_actor_type = caf::stateful_actor<driver_state>;
//
// caf::behavior driver(driver_actor_type* self, const actor& sink,
//                      bool restartable) {
//   self->state.restartable = restartable;
//   auto ptr = caf::attach_stream_source(
//     self,
//     // Destination.
//     sink,
//     // Initialize state.
//     [](caf::unit_t&) {
//       // nop
//     },
//     // Get next element.
//     [=](caf::unit_t&, caf::downstream<element_type>& out, size_t num) {
//       auto& xs = self->state.xs;
//       auto n = std::min(num, xs.size());
//       if (n == 0)
//         return;
//       for (size_t i = 0u; i < n; ++i)
//         out.push(xs[i]);
//       xs.erase(xs.begin(), xs.begin() + static_cast<ptrdiff_t>(n));
//     },
//     // Did we reach the end?
//     [=](const caf::unit_t&) {
//       auto& st = self->state;
//       return !st.restartable && st.xs.empty();
//     }
//   ).ptr();
//   return {
//     [=](atom::restart) {
//       self->state.reset();
//       self->state.restartable = false;
//       ptr->push();
//     },
//   };
// }
//
// struct consumer_state {
//   std::vector<element_type> xs;
//   static inline const char* name = "consumer";
// };
//
// using consumer_actor_type = caf::stateful_actor<consumer_state>;
//
// caf::behavior consumer(consumer_actor_type* self, filter_type filter,
//                        const actor& src) {
//   self->send(self * src, atom::join_v, std::move(filter));
//   return {
//     [=](const endpoint::stream_type& in) {
//       caf::attach_stream_sink(
//         self,
//         // Input stream.
//         in,
//         // Initialize state.
//         [](caf::unit_t&) {
//           // nop
//         },
//         // Process single element.
//         [=](caf::unit_t&, element_type x) {
//           self->state.xs.emplace_back(std::move(x));
//         },
//         // Cleanup.
//         [](caf::unit_t&) {
//           // nop
//         }
//       );
//     },
//     [=](atom::get) {
//       return self->state.xs;
//     },
//   };
// }
//
// struct fixture : base_fixture {
//   // Returns the core manager for given actor.
//   auto& state(caf::actor hdl) {
//     return deref<core_actor_type>(hdl).state;
//   }
//
//   // Returns the recorded consumer log for given actor.
//   auto& log(caf::actor hdl) {
//     return deref<consumer_actor_type>(hdl).state.xs;
//   }
//
//   auto id(caf::actor hdl) {
//     return state(hdl).id();
//   }
//
//   fixture() {
//     using caf::make_uri;
//     auto spawn_core = [&](auto id) {
//       broker_options opts;
//       opts.disable_ssl = true;
//       auto hdl = sys.spawn<core_actor_type>(filter_type{"a", "b", "c"});
//       anon_send(core1, atom::no_events_v);
//       run();
//       state(hdl).id(make_node_id(unbox(id)));
//       if (state(hdl).filter() != filter_type{"a", "b", "c"})
//         FAIL("core " << id << " reports wrong filter: " << state(hdl).filter());
//       return hdl;
//     };
//     core1 = spawn_core(make_uri("test:core1"));
//     core2 = spawn_core(make_uri("test:core2"));
//     core3 = spawn_core(make_uri("test:core3"));
//   }
//
//   ~fixture() {
//     for (auto& hdl : {core1, core2, core3})
//       anon_send_exit(hdl, caf::exit_reason::user_shutdown);
//   }
//
//   template <class... Ts>
//   void stop(Ts&&... xs) {
//     (anon_send_exit(xs, caf::exit_reason::user_shutdown), ...);
//   }
//
//   caf::actor core1;
//   caf::actor core2;
//   caf::actor core3;
// };
//
// static constexpr bool T = true;
//
// static constexpr bool F = false;
//
// } // namespace
//
// FIXTURE_SCOPE(local_tests, fixture)
//
// // Simulates a simple setup with two cores, where data flows from core1 to
// // core2.
// TEST(local_peers) {
//   MESSAGE("connect a consumer (leaf) to core2");
//   auto leaf = sys.spawn(consumer, filter_type{"b"}, core2);
//   MESSAGE("core1: " << to_string(core1));
//   MESSAGE("core2: " << to_string(core2));
//   MESSAGE("leaf: " << to_string(leaf));
//   run();
//   CHECK_EQUAL(state(core1).worker_manager().num_paths(), 0u);
//   CHECK_EQUAL(state(core2).worker_manager().num_paths(), 1u);
//   MESSAGE("trigger handshake between peers");
//   inject((atom::peer, node_id, actor),
//          from(self).to(core1).with(atom::peer_v, id(core2), core2));
//   run();
//   MESSAGE("core1 & core2 should report each other as peered");
//   using actor_list = std::vector<actor>;
//   CHECK_EQUAL(state(core1).peer_handles(), actor_list({core2}));
//   CHECK_EQUAL(state(core1).peer_filter(id(core2)), filter_type({"a", "b", "c"}));
//   CHECK_EQUAL(state(core2).peer_handles(), actor_list({core1}));
//   CHECK_EQUAL(state(core2).peer_filter(id(core1)), filter_type({"a", "b", "c"}));
//   return;
//   MESSAGE("spin up driver on core1");
//   auto d1 = sys.spawn(driver, core1, false);
//   MESSAGE("driver: " << to_string(d1));
//   run();
//   MESSAGE("check log of the consumer after the driver is done");
//   CHECK_EQUAL(log(leaf), data_msgs({{"b", T}, {"b", F}, {"b", T}, {"b", F}}));
//   MESSAGE("send message 'directly' from core1 to core2");
//   inject((atom::publish, endpoint_info, data_message),
//          from(self).to(core1).with(atom::publish_v,
//                                    endpoint_info{id(core2), caf::none},
//                                    make_data_message(topic("b"), data{true})));
//   run();
//   MESSAGE("check log of the consumer again");
//   CHECK_EQUAL(log(leaf),
//               data_msgs({{"b", T}, {"b", F}, {"b", T}, {"b", F}, {"b", T}}));
//   MESSAGE("unpeer core1 from core2");
//   anon_send(core1, atom::unpeer_v, core2);
//   run();
//   MESSAGE("check whether both core1 and core2 report no more peers");
//   CHECK_EQUAL(state(core1).peer_handles().size(), 0u);
//   CHECK_EQUAL(state(core2).peer_handles().size(), 0u);
// }
//
// // Simulates a simple triangle setup where core1 peers with core2, and core2
// // peers with core3. Data flows from core1 to core2 and core3.
// TEST(triangle_peering) {
//   MESSAGE("connect consumers for topic 'b' to all cores");
//   // The consumer at core1 never receives any data, because data isn't forwarded
//   // to local subscribers.
//   auto leaf1 = sys.spawn(consumer, filter_type{"b"}, core1);
//   auto leaf2 = sys.spawn(consumer, filter_type{"b"}, core2);
//   auto leaf3 = sys.spawn(consumer, filter_type{"b"}, core3);
//   run();
//   MESSAGE("initiate handshake between core1 and core2");
//   inject((atom::peer, node_id, actor),
//          from(self).to(core1).with(atom::peer_v, id(core2), core2));
//   // Check if core1 reports a pending peer.
//   CHECK_EQUAL(state(core1).pending_connections().count(id(core2)), 1u);
//   run();
//   MESSAGE("initiate handshake between core2 and core3");
//   inject((atom::peer, node_id, actor),
//          from(self).to(core2).with(atom::peer_v, id(core3), core3));
//   CHECK_EQUAL(state(core2).pending_connections().count(id(core3)), 1u);
//   run();
//   MESSAGE("check if all cores properly report the peering setup");
//   CHECK(state(core1).pending_connections().empty());
//   CHECK(state(core2).pending_connections().empty());
//   CHECK(state(core3).pending_connections().empty());
//   CHECK(state(core1).connected_to(core2));
//   CHECK(state(core2).connected_to(core1));
//   CHECK(state(core2).connected_to(core3));
//   CHECK(state(core3).connected_to(core2));
//   CHECK(not state(core1).connected_to(core3));
//   CHECK(not state(core3).connected_to(core1));
//   MESSAGE("attach and run driver on core1");
//   sys.spawn(driver, core1, false);
//   run();
//   MESSAGE("check log of the consumers");
//   CHECK(log(leaf1).empty());
//   CHECK_EQUAL(log(leaf2), data_msgs({{"b", T}, {"b", F}, {"b", T}, {"b", F}}));
//   CHECK_EQUAL(log(leaf3), data_msgs({{"b", T}, {"b", F}, {"b", T}, {"b", F}}));
//   stop(leaf1, leaf2, leaf3);
// }
//
// // Simulates a simple setup where core1 peers with core2 and starts sending
// // data. After receiving a couple of messages, core2 terminates and core3
// // starts peering. Core3 must receive all remaining messages.
// // peers with core3. Data flows from core1 to core2 and core3.
// TEST(sequenced_peering) {
//   MESSAGE("connect consumers for topic 'b' to cores 2 and 3");
//   // The consumer at core1 never receives any data, because data isn't forwarded
//   // to local subscribers.
//   auto leaf1 = sys.spawn(consumer, filter_type{"b"}, core2);
//   auto leaf2 = sys.spawn(consumer, filter_type{"b"}, core3);
//   run();
//   MESSAGE("peer core1 to core2");
//   inject((atom::peer, node_id, actor),
//          from(self).to(core1).with(atom::peer_v, id(core2), core2));
//   run();
//   CHECK(state(core1).connected_to(core2));
//   CHECK(state(core2).connected_to(core1));
//   CHECK(not state(core1).connected_to(core3));
//   CAF_MESSAGE("run the driver and check logs of the consumers");
//   auto d1 = sys.spawn(driver, core1, true);
//   run();
//   CHECK_EQUAL(log(leaf1), data_msgs({{"b", T}, {"b", F}, {"b", T}, {"b", F}}));
//   CHECK(log(leaf2).empty());
//   CAF_MESSAGE("kill core2 and make sure core1 no longer sees any peers");
//   anon_send_exit(core2, caf::exit_reason::kill);
//   run();
//   CHECK(state(core1).tbl().empty());
//   CHECK(state(core1).pending_connections().empty());
//   MESSAGE("peer core3 to core1");
//   inject((atom::peer, node_id, actor),
//          from(self).to(core3).with(atom::peer_v, id(core1), core1));
//   run();
//   CHECK(state(core1).connected_to(core3));
//   CHECK(state(core3).connected_to(core1));
//   CHECK(not state(core1).connected_to(core2));
//   CAF_MESSAGE("restart driver and check the logs again");
//   anon_send(d1, atom::restart_v);
//   run();
//   CHECK_EQUAL(log(leaf1), data_msgs({{"b", T}, {"b", F}, {"b", T}, {"b", F}}));
//   CHECK_EQUAL(log(leaf2), data_msgs({{"b", T}, {"b", F}, {"b", T}, {"b", F}}));
//   stop(d1, leaf1, leaf2);
// }
//
// CAF_TEST_FIXTURE_SCOPE_END()

TEST(todo) {
  MESSAGE("implement me");
}
