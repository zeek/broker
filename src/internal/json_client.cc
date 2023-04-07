#include "broker/internal/json_client.hh"

#include "broker/error.hh"
#include "broker/internal/type_id.hh"
#include "broker/message.hh"
#include "broker/version.hh"

#include <caf/cow_string.hpp>
#include <caf/cow_tuple.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/scheduled_actor/flow.hpp>
#include <caf/unordered_flat_map.hpp>

using namespace std::literals;

using string_map = caf::unordered_flat_map<std::string, std::string>;

namespace broker::internal {

namespace {

constexpr std::string_view default_serialization_failed_error_str = R"_({
  "type": "error",
  "code": "serialization_failed",
  "context": "internal JSON writer error"
})_";

/// Catches errors by converting them into complete events instead.
struct handshake_step {
  using input_type = caf::cow_string;

  using output_type = caf::cow_string;

  json_client_state* state;

  json_client_state::out_t push_to_ws; // Our push handle to the WebSocket.

  using pull_from_core_t = caf::async::consumer_resource<data_message>;

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
  writer.mapper(&mapper);
  writer.skip_object_type_annotation(true);
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
  auto resources = make_spsc_buffer_resource<data_message>();
  auto& [core_pull, core_push] = resources;
  // Read from the WebSocket, push to core (core_push).
  self //
    ->make_observable()
    .from_resource(std::move(in)) // Read all input text messages.
    .transform(handshake_step{this, std::move(out), core_pull}) // Calls init().
    .do_finally([this] { ctrl_msgs.close(); })
    // Parse all JSON coming in and forward them to the core.
    .flat_map([this, n = 0](const caf::cow_string& str) mutable {
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
          auto json = render_error(enum_str(ec::deserialization_failed), ctx);
          ctrl_msgs.push(caf::cow_string{std::move(json)});
        }
      } else {
        // Failed to parse JSON.
        auto ctx = std::to_string(n);
        ctx.insert(0, "input #");
        ctx += " contained malformed JSON -> ";
        ctx += to_string(reader.get_error());
        auto json = render_error(enum_str(ec::deserialization_failed), ctx);
        ctrl_msgs.push(caf::cow_string{std::move(json)});
      }
      return result;
    })
    .subscribe(core_push);
}

json_client_state::~json_client_state() {
  for (auto& sub : subscriptions)
    sub.dispose();
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

void json_client_state::init(
  const filter_type& filter, const out_t& out,
  caf::async::consumer_resource<data_message> core_pull1) {
  using caf::async::make_spsc_buffer_resource;
  // Pull data from the core and forward as JSON.
  if (!filter.empty()) {
    // Note: structured bindings with values confuses clang-tidy's leak checker.
    auto resources = make_spsc_buffer_resource<data_message>();
    auto& [core_pull2, core_push2] = resources;
    auto core_json = //
      self->make_observable()
        .from_resource(core_pull2)
        .map([this](const data_message& msg) -> caf::cow_string {
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
    auto sub = ctrl_msgs.as_observable().merge(core_json).subscribe(out);
    subscriptions.push_back(std::move(sub));
    caf::anon_send(core, atom::attach_client_v, addr, "web-socket"s, filter,
                   std::move(core_pull1), std::move(core_push2));
  } else {
    auto sub = ctrl_msgs.as_observable().subscribe(out);
    subscriptions.push_back(std::move(sub));
    caf::anon_send(core, atom::attach_client_v, addr, "web-socket"s,
                   filter_type{}, std::move(core_pull1),
                   caf::async::producer_resource<data_message>{});
  }
  // Setup complete. Send ACK to the client.
  ctrl_msgs.push(caf::cow_string{render_ack()});
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
