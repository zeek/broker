#define SUITE flow_controller

// #include "broker/flow_controller.hh"

#include "test.hh"

#include <caf/flow/async/publisher.hpp>
#include <caf/flow/publisher.hpp>
#include <caf/scheduled_actor/flow.hpp>

using namespace broker;

// namespace {
//
// /// Maps topics to integer IDs and vice versa.
// class topic_registry {
// public:
//   topic_id id_of(const topic& what) {
//     std::unique_lock guard{mtx_};
//     for (auto index = size_t{0}; index < topics_.size(); ++index)
//       if (*topics_[index] == what)
//         return index;
//     auto result = topics_.size();
//     topics_.emplace_back(std::make_unique<topic>(what));
//     return result;
//   }
//
//   const topic& value_of(topic_id id) {
//     std::unique_lock guard{mtx_};
//     return *topics_[id];
//   }
//
// private:
//   mutable std::mutex mtx_;
//   std::vector<std::unique_ptr<topic>> topics_;
// };
//
// using topic_registry_ptr = std::shared_ptr<topic_registry>;
//
// class flow_controller {
// public:
//   caf::flow::async::publisher<int> gimme_gimme(caf::uuid peer);
// };
//
// struct fixture : test_coordinator_fixture<> {};
//
// /*
// class payload {
// public:
//   explicit payload(const caf::byte_buffer& buf) {
//     bytes_ = std::make_shared<caf::byte_buffer>(buf);
//   }
//
//   payload() noexcept = default;
//
//   payload(payload&&) noexcept = default;
//
//   payload& operator=(payload&&) noexcept = default;
//
//   payload(const payload&) noexcept = default;
//
//   payload& operator=(const payload&) noexcept = default;
//
//   auto bytes() const noexcept {
//     return bytes_ ? caf::span<const caf::byte>{*bytes_}
//                   : caf::span<const caf::byte>{};
//   }
//
// private:
//   std::shared_ptr<caf::byte_buffer> bytes_;
// };
// */
//
// struct serializer_state {
//   caf::behavior make_behavior() {
//     return {};
//   }
// };
//
// using detail::packed_message;
// using detail::packed_message_type;
//
// using serializer_actor = caf::stateful_actor<serializer_state>;
//
// using serializer_stage_base
//   = caf::flow::buffered_processor<node_message_content, packed_message>;
//
// class serializer_stage : public serializer_stage_base {
// public:
//   using super = serializer_stage_base;
//
//   serializer_stage(caf::flow::coordinator* ctx, topic_registry_ptr topics)
//     : super(ctx), topics_(std::move(topics)) {
//     // nop
//   }
//
//   void on_next(caf::span<const node_message_content> items) override {
//     auto f = [this](const auto& content) {
//       using content_type = std::decay_t<decltype(content)>;
//       buf_.clear();
//       caf::binary_serializer sink{nullptr, buf_};
//       if constexpr (std::is_same_v<content_type, data_message>) {
//         if (!sink.apply(get_data(content))) {
//           this->abort(sink.get_error());
//           return;
//         }
//         this->append_to_buf(packed_message{topics_->id_of(get_topic(content)),
//                                            packed_message_type::data, buf_});
//       } else {
//         static_assert(std::is_same_v<content_type, command_message>);
//         if (!sink.apply(get_command(content))) {
//           this->abort(sink.get_error());
//           return;
//         }
//         this->append_to_buf(packed_message{topics_->id_of(get_topic(content)),
//                                            packed_message_type::command, buf_});
//       }
//     };
//     for (auto&& item : items)
//       caf::visit(f, item);
//     this->try_push();
//   }
//
// private:
//   caf::byte_buffer buf_;
//   topic_registry_ptr topics_;
// };
//
// using deserializer_stage_base
//   = caf::flow::buffered_processor<packed_message, node_message_content>;
//
// class deserializer_stage : public deserializer_stage_base {
// public:
//   using super = deserializer_stage_base;
//
//   deserializer_stage(caf::flow::coordinator* ctx, topic_registry_ptr topics)
//     : super(ctx), topics_(std::move(topics)) {
//     // nop
//   }
//
//   void on_next(caf::span<const packed_message> items) override {
//     for (auto&& item : items) {
//       const auto& [tid, type, buf] = item.data();
//       auto& tval = topics_->value_of(tid);
//       caf::binary_deserializer src{nullptr, buf};
//       switch (type) {
//         case packed_message_type::data: {
//           data msg;
//           if (!src.apply(msg)) {
//             this->abort(src.get_error());
//             return;
//           }
//           super::append_to_buf(make_data_message(tval, std::move(msg)));
//           break;
//         }
//         default: {
//           BROKER_ASSERT(type == packed_message_type::command);
//           internal_command msg;
//           if (!src.apply(msg)) {
//             this->abort(src.get_error());
//             return;
//           }
//           super::append_to_buf(make_command_message(tval, std::move(msg)));
//           break;
//         }
//       }
//     }
//     super::try_push();
//   }
//
// private:
//   topic_registry_ptr topics_;
// };
//
// } // namespace
//
// FIXTURE_SCOPE(flow_controller_tests, fixture)
//
// TEST(todo) {
//   auto topics = std::make_shared<topic_registry>();
//   std::vector<node_message_content> test_data;
//   test_data.emplace_back(make_data_message("/foo/bar", 123));
//   test_data.emplace_back(make_data_message("/foo/baz", 234));
//   test_data.emplace_back(make_data_message("/foo/bar", 321));
//   test_data.emplace_back(make_data_message("/foo/baz", 432));
//   auto buf = std::make_shared<std::vector<node_message_content>>();
//   // clang-format off
//   // Stream the content of test_data to later stages.
//   caf::flow::async::publisher_from(sys, [topics, test_data](auto* self) {
//     return self->make_publisher()->iterate(test_data);
//   })
//   // Transform data messages to payloads.
//   ->subscribe_with(sys, [topics](auto* self, auto&& in) {
//     return in->template subscribe_with_new<serializer_stage>(self, topics);
//   })
//   // Transform payloads back to data messages.
//   ->subscribe_with(sys, [topics, buf](auto* self, auto&& in) {
//     in
//     ->template subscribe_with_new<deserializer_stage>(self, topics)
//     ->subscribe([buf](const node_message_content& x) {
//       buf->emplace_back(x);
//     });
//   });
//   // clang-format on
//   run();
//   if (CHECK_EQUAL(test_data.size(), buf->size())) {
//     auto eq = [](const auto& lhs, const auto& rhs) {
//       using lhs_type = std::decay_t<decltype(lhs)>;
//       using rhs_type = std::decay_t<decltype(rhs)>;
//       if constexpr (std::is_same_v<lhs_type, rhs_type> //
//                     && std::is_same_v<lhs_type, data_message>) {
//         return lhs.data() == rhs.data();
//       } else {
//         return false;
//       }
//     };
//     for (size_t index = 0; index < test_data.size(); ++index)
//       CHECK(caf::visit(eq, test_data[index], (*buf)[index]));
//   }
// }
//
// FIXTURE_SCOPE_END()

TEST(todo) {
}
