#include "broker/internal/json_client.hh"

#include "broker/data_envelope.hh"
#include "broker/defaults.hh"
#include "broker/envelope.hh"
#include "broker/error.hh"
#include "broker/expected.hh"
#include "broker/format/json.hh"
#include "broker/internal/json.hh"
#include "broker/internal/type_id.hh"
#include "broker/version.hh"

#include <caf/cow_string.hpp>
#include <caf/cow_tuple.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/json_object.hpp>
#include <caf/json_value.hpp>
#include <caf/scheduled_actor/flow.hpp>
#include <caf/unordered_flat_map.hpp>

using namespace std::literals;

namespace broker::internal {

namespace {

/// Catches errors by converting them into complete events instead.
struct handshake_step {
  using input_type = caf::cow_string;

  using output_type = caf::cow_string;

  json_client_state* state;

  json_client_state::out_t push_to_ws; // Our push handle to the WebSocket.

  using pull_from_core_t = caf::async::consumer_resource<data_envelope_ptr>;

  pull_from_core_t pull_from_core; // Allows the core to read our data.

  bool initialized = false;

  handshake_step(json_client_state* state_ptr,
                 json_client_state::out_t push_to_ws,
                 pull_from_core_t pull_from_core)
    : state(state_ptr),
      push_to_ws(std::move(push_to_ws)),
      pull_from_core(std::move(pull_from_core)) {
    // nop
  }

  template <class Next, class... Steps>
  bool on_next(const input_type& item, Next& next, Steps&... steps) {
    if (initialized) {
      return next.on_next(item, steps...);
    } else {
      filter_type filter;
      state->reader.load(item.str());
      if (!state->reader.apply(filter)) {
        // Received malformed input: drop remaining input and quit.
        auto err = caf::make_error(caf::sec::invalid_argument,
                                   "first message must contain a filter");
        next.on_error(err, steps...);
        push_to_ws = nullptr;
        pull_from_core = nullptr;
        return false;
      } else {
        initialized = true;
        // Ok, set up the actual pipeline and connect to the core.
        state->init(filter, push_to_ws, std::move(pull_from_core));
        return true;
      }
    }
  }

  template <class Next, class... Steps>
  void on_complete(Next& next, Steps&... steps) {
    next.on_complete(steps...);
  }

  template <class Next, class... Steps>
  void on_error(const caf::error& what, Next& next, Steps&... steps) {
    next.on_error(what, steps...);
  }
};

} // namespace

json_client_state::json_client_state(caf::event_based_actor* selfptr,
                                     endpoint_id this_node, caf::actor core_hdl,
                                     network_info ws_addr, in_t in, out_t out)
  : self(selfptr),
    id(this_node),
    core(std::move(core_hdl)),
    addr(std::move(ws_addr)),
    ctrl_msgs(selfptr) {
  reader.mapper(&mapper);
  using caf::cow_string;
  using caf::flow::observable;
  using head_and_tail_t = caf::cow_tuple<cow_string, observable<cow_string>>;
  self->monitor(core);
  self->set_down_handler([this](const caf::down_msg& msg) { //
    on_down_msg(msg);
  });
  // Connects us to the core.
  using caf::async::make_spsc_buffer_resource;
  // Note: structured bindings with values confuses clang-tidy's leak checker.
  auto resources = make_spsc_buffer_resource<data_envelope_ptr>();
  auto& [core_pull, core_push] = resources;
  // Read from the WebSocket, push to core (core_push).
  self //
    ->make_observable()
    .from_resource(std::move(in)) // Read all input text messages.
    .transform(handshake_step{this, std::move(out), core_pull}) // Calls init().
    .do_finally([this] { ctrl_msgs.close(); })
    // Parse all JSON coming in and forward them to the core.
    .map([this, n = 0](const caf::cow_string& cow_str) mutable {
      ++n;
      auto send_error = [this, n](auto&&... args) {
        auto ctx = "input #" + std::to_string(n);
        ctx += ' ';
        (ctx += ... += args);
        auto json = render_error(enum_str(ec::deserialization_failed), ctx);
        ctrl_msgs.push(caf::cow_string{std::move(json)});
      };
      // Parse the received JSON.
      auto val = caf::json_value::parse_shallow(cow_str.str());
      if (!val) {
        send_error("contained malformed JSON -> ", to_string(val.error()));
        return data_envelope_ptr{};
      }
      auto obj = val->to_object();
      // Try to convert the JSON to our internal representation.
      buf.clear();
      if (auto err = internal::json::data_message_to_binary(obj, buf)) {
        send_error("contained invalid data");
        return data_envelope_ptr{};
      }
      // Turn the binary data into a data envelope.
      auto maybe_msg = data_envelope::deserialize(
        id, endpoint_id::nil(), defaults::ttl,
        caf::to_string(obj.value("topic").to_string()), buf.data(), buf.size());
      if (!maybe_msg) {
        send_error("caused an internal error -> ",
                   to_string(maybe_msg.error()));
        return data_envelope_ptr{};
      }
      return std::move(*maybe_msg);
    })
    .filter([](const data_envelope_ptr& ptr) { return ptr != nullptr; })
    .subscribe(core_push);
}

json_client_state::~json_client_state() {
  for (auto& sub : subscriptions)
    sub.dispose();
}

std::string json_client_state::render_error(std::string_view code,
                                            std::string_view context) {
  using format::json::v1::append_field;
  json_buf.clear();
  auto out = std::back_inserter(json_buf);
  *out++ = '{';
  out = append_field("type", "error", out);
  *out++ = ',';
  out = append_field("code", code, out);
  *out++ = ',';
  out = append_field("context", context, out);
  *out++ = '}';
  return std::string{json_buf.data(), json_buf.size()};
}

std::string json_client_state::render_ack() {
  using format::json::v1::append_field;
  json_buf.clear();
  auto out = std::back_inserter(json_buf);
  *out++ = '{';
  out = append_field("type", "ack", out);
  *out++ = ',';
  out = append_field("endpoint", to_string(id), out);
  *out++ = ',';
  out = append_field("version", version::string(), out);
  *out++ = '}';
  return std::string{json_buf.data(), json_buf.size()};
}

void json_client_state::init(
  const filter_type& filter, const out_t& out,
  caf::async::consumer_resource<data_envelope_ptr> core_pull1) {
  using caf::async::make_spsc_buffer_resource;
  // Pull data from the core and forward as JSON.
  if (!filter.empty()) {
    // Note: structured bindings with values confuses clang-tidy's leak checker.
    auto resources = make_spsc_buffer_resource<data_envelope_ptr>();
    auto& [core_pull2, core_push2] = resources;
    auto core_json = //
      self->make_observable()
        .from_resource(core_pull2)
        .map([this](const data_envelope_ptr& msg) -> caf::cow_string {
          json_buf.clear();
          format::json::v1::encode(msg, std::back_inserter(json_buf));
          return caf::cow_string{std::string{json_buf.begin(), json_buf.end()}};
        })
        .as_observable();
    auto sub = ctrl_msgs.as_observable().merge(core_json).subscribe(out);
    subscriptions.push_back(std::move(sub));
    caf::anon_send(core, atom::attach_client_v, addr, "web-socket"s, filter,
                   std::move(core_pull1), std::move(core_push2));
  } else {
    auto sub = ctrl_msgs.as_observable().subscribe(out);
    subscriptions.push_back(std::move(sub));
    caf::anon_send(core, atom::attach_client_v, addr, "web-socket"s,
                   filter_type{}, std::move(core_pull1),
                   caf::async::producer_resource<data_envelope_ptr>{});
  }
  // Setup complete. Send ACK to the client.
  ctrl_msgs.push(caf::cow_string{render_ack()});
}

void json_client_state::on_down_msg(const caf::down_msg& msg) {
  for (auto& sub : subscriptions)
    sub.dispose();
  subscriptions.clear();
  self->quit();
}

} // namespace broker::internal
