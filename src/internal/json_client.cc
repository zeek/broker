#include "broker/internal/json_client.hh"

#include "broker/error.hh"
#include "broker/internal/type_id.hh"
#include "broker/message.hh"
#include "broker/version.hh"

#include <caf/cow_string.hpp>
#include <caf/cow_tuple.hpp>
#include <caf/detail/unordered_flat_map.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/flow/merge.hpp>
#include <caf/scheduled_actor/flow.hpp>

using namespace std::literals;

// Note: no longer a `detail` in CAF 0.19, so it's safe to use.
using string_map = caf::detail::unordered_flat_map<std::string, std::string>;

namespace broker::internal {

namespace {

constexpr std::string_view default_serialization_failed_error_str = R"_({
  "type": "error",
  "code": "serialization_failed",
  "context": "internal JSON writer error"
})_";

} // namespace

json_client_state::json_client_state(caf::event_based_actor* selfptr,
                                     endpoint_id this_node, caf::actor core,
                                     network_info addr, in_t in, out_t out)
  : self(selfptr), id(this_node) {
  reader.mapper(&mapper);
  writer.mapper(&mapper);
  writer.skip_object_type_annotation(true);
  using caf::cow_string;
  using caf::flow::observable;
  using head_and_tail_t = caf::cow_tuple<cow_string, observable<cow_string>>;
  self->monitor(core);
  self->set_down_handler([this](const caf::down_msg& msg) { //
    on_down_msg(msg);
  });
  self //
    ->make_observable()
    .from_resource(in) // Read all input text messages.
    .head_and_tail()   // The first message contains the filter.
    .for_each([this, out, core, addr](const head_and_tail_t& head_and_tail) {
      auto& [head, tail] = head_and_tail.data();
      filter_type filter;
      reader.load(head.str());
      if (!reader.apply(filter)) {
        // Received malformed input: drop remaining input and quit.
        auto err = caf::make_error(caf::sec::invalid_argument,
                                   "first message must contain a filter");
        using impl_t = caf::flow::observable_error_impl<cow_string>;
        auto obs = caf::make_counted<impl_t>(self, std::move(err));
        obs->as_observable().subscribe(out);
      } else {
        // Ok, set up the actual pipeline and connect to the core.
        run(core, std::move(filter), addr, tail, out);
      }
    });
}

std::string json_client_state::render_error(std::string_view code,
                                            std::string_view context) {
  string_map obj;
  obj.reserve(3);
  obj["type"s] = "error"s;
  obj["code"s] = code;
  obj["context"s] = context;
  return render(obj);
}

std::string json_client_state::render_ack() {
  string_map obj;
  obj.reserve(3);
  obj["type"s] = "ack"sv;
  obj["endpoint"s] = to_string(id);
  obj["version"s] = version::string();
  return render(obj);
}

struct const_data_message_decorator {
  const topic& t;
  const data& d;
};

const_data_message_decorator decorated(const data_message& msg) {
  auto& [t, d] = msg.data();
  return const_data_message_decorator{t, d};
}

template <class Inspector>
bool inspect(Inspector& f, const_data_message_decorator& x) {
  static_assert(!Inspector::is_loading);
  auto do_inspect = [&f, &x](const auto& val) -> bool {
    json_type_mapper tm;
    using val_t = std::decay_t<decltype(val)>;
    auto type = "data-message"s;
    auto dtype = to_string(tm(caf::type_id_v<val_t>));
    // Note: const_cast is safe since we assert that the inspector is saving.
    return f.object(x).fields(f.field("type", type),
                              f.field("topic", const_cast<topic&>(x.t)),
                              f.field("@data-type", dtype),
                              f.field("data", const_cast<val_t&>(val)));
  };
  return visit(do_inspect, x.d);
}

void json_client_state::run(caf::actor core, filter_type filter,
                            network_info addr,
                            caf::flow::observable<caf::cow_string> in,
                            out_t out) {
  using caf::async::make_spsc_buffer_resource;
  // Allows us to push control messages to the client.
  caf::flow::buffered_observable_impl_ptr<caf::cow_string> ctrl_msgs;
  ctrl_msgs.emplace(self);
  // Parse all JSON coming in and forward them to the core.
  auto [core_pull1, core_push1] = make_spsc_buffer_resource<data_message>();
  auto in_sub
    = in.flat_map_optional([this, ctrl_msgs,
                            n = 0](const caf::cow_string& str) mutable {
          ++n;
          std::optional<data_message> result;
          reader.reset();
          if (reader.load(str)) {
            using std::get;
            data_message msg;
            auto decorator = decorated(msg);
            if (reader.apply(decorator)) {
              // Success: set the result to push it to the core actor.
              result = std::move(msg);
            } else {
              // Failed to apply the JSON reader. Send error to client.
              auto ctx = std::to_string(n);
              ctx.insert(0, "input #");
              ctx += " contained invalid data -> ";
              ctx += to_string(reader.get_error());
              auto json = render_error(enum_str(ec::deserialization_failed),
                                       ctx);
              ctrl_msgs->append_to_buf(caf::cow_string{std::move(json)});
              ctrl_msgs->try_push();
            }
          } else {
            // Failed to parse JSON.
            auto ctx = std::to_string(n);
            ctx.insert(0, "input #");
            ctx += " contained malformed JSON -> ";
            ctx += to_string(reader.get_error());
            auto json = render_error(enum_str(ec::deserialization_failed), ctx);
            ctrl_msgs->append_to_buf(caf::cow_string{std::move(json)});
            ctrl_msgs->try_push();
          }
          return result;
        })
        .subscribe(core_push1);
  subscriptions.push_back(in_sub);
  // Pull data from the core and forward as JSON.
  if (!filter.empty()) {
    auto [core_pull2, core_push2] = make_spsc_buffer_resource<data_message>();
    auto core_json = //
      self->make_observable()
        .from_resource(core_pull2)
        .map(
          [this](const data_message& msg) -> caf::cow_string {
            writer.reset();
            auto decorator = decorated(msg);
            if (writer.apply(decorator)) {
              // Serialization OK, forward message to client.
              auto json = writer.str();
              auto str = std::string{json.begin(), json.end()};
              return caf::cow_string{std::move(str)};
            } else {
              // Report internal error to client.
              auto ctx = to_string(writer.get_error().context());
              auto str = render_error(enum_str(ec::serialization_failed), ctx);
              return caf::cow_string{std::move(str)};
            }
          })
        .as_observable();
    auto sub = caf::flow::merge(ctrl_msgs->as_observable(), core_json) //
                 .subscribe(out);
    subscriptions.push_back(std::move(sub));
    caf::anon_send(core, atom::attach_client_v, addr, "web-socket"s, filter,
                   core_pull1, core_push2);
  } else {
    auto sub = ctrl_msgs->as_observable().subscribe(out);
    subscriptions.push_back(std::move(sub));
    caf::anon_send(core, atom::attach_client_v, addr, "web-socket"s,
                   filter_type{}, core_pull1,
                   caf::async::producer_resource<data_message>{});
  }
  // Setup complete. Send ACK to the client.
  ctrl_msgs->append_to_buf(caf::cow_string{render_ack()});
  ctrl_msgs->try_push();
}

std::string_view json_client_state::default_serialization_failed_error() {
  return default_serialization_failed_error_str;
}

void json_client_state::on_down_msg(const caf::down_msg& msg) {
  for (auto& sub : subscriptions)
    sub.dispose();
  subscriptions.clear();
  self->quit();
}

} // namespace broker::internal
