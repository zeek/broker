#include "broker/internal/prometheus.hh"

#include <memory>
#include <string_view>

#include <caf/actor_system_config.hpp>
#include <caf/string_algorithms.hpp>

#include "broker/internal/logger.hh"
#include "broker/internal/metric_exporter.hh"
#include "broker/message.hh"

using namespace std::literals;

namespace broker::internal {

namespace {

using std::string_view;

// Cap incoming HTTP requests.
constexpr size_t max_request_size = 512ul * 1024ul;

// A GET request for Prometheus metrics.
constexpr string_view prom_request_start = "GET /metrics HTTP/1.";

// A GET request for JSON-formatted status snapshots.
constexpr string_view status_request_start = "GET /v1/status/json HTTP/1.";

// HTTP response for requests that exceed the size limit.
constexpr string_view request_too_large =
  "HTTP/1.1 413 Request Entity Too Large\r\n"
  "Connection: Closed\r\n\r\n";

// HTTP response for requests that don't start with "GET /metrics HTTP/1".
constexpr string_view request_not_supported = "HTTP/1.1 501 Not Implemented\r\n"
                                              "Connection: Closed\r\n\r\n";

// HTTP header when sending a plain text.
constexpr string_view request_ok_text = "HTTP/1.1 200 OK\r\n"
                                        "Content-Type: text/plain\r\n"
                                        "Connection: Closed\r\n\r\n";

// HTTP header when sending a JSON.
constexpr string_view request_ok_json = "HTTP/1.1 200 OK\r\n"
                                        "Content-Type: application/json\r\n"
                                        "Connection: Closed\r\n\r\n";

} // namespace

// -- constructors, destructors, and assignment operators ----------------------

prometheus_actor::prometheus_actor(caf::actor_config& cfg,
                                   caf::io::doorman_ptr ptr, caf::actor core)
  : super(cfg), core_(std::move(core)) {
  filter_ = caf::get_or(config(), "broker.metrics.import.topics",
                        filter_type{});
  add_doorman(std::move(ptr));
}

// -- overrides ----------------------------------------------------------------

void prometheus_actor::on_exit() {
  requests_.clear();
  core_ = nullptr;
  exporter_.reset();
}

const char* prometheus_actor::name() const {
  return "broker.telemetry-prometheus";
}

caf::behavior prometheus_actor::make_behavior() {
  if (!core_) {
    BROKER_ERROR("started a Prometheus actor with an invalid core handle");
    return {};
  }
  monitor(core_);
  set_down_handler([this](const caf::down_msg& msg) {
    if (msg.source == core_) {
      BROKER_INFO("the core terminated:" << msg.reason);
      quit(msg.reason);
    }
  });
  if (!filter_.empty()) {
    BROKER_INFO("collect remote metrics from topics" << filter_);
    send(core_, atom::join_v, filter_);
  }
  auto bhvr = caf::message_handler{
    [this](const caf::io::new_data_msg& msg) {
      // Ignore data we're no longer interested in.
      auto iter = requests_.find(msg.handle);
      if (iter == requests_.end() || iter->second.async_id != 0)
        return;
      auto& req = iter->second.buf;
      if (req.size() + msg.buf.size() > max_request_size) {
        write(msg.handle, caf::as_bytes(caf::make_span(request_too_large)));
        flush_and_close(msg.handle);
        return;
      }
      req.insert(req.end(), msg.buf.begin(), msg.buf.end());
      auto req_str = string_view{reinterpret_cast<char*>(req.data()),
                                 req.size()};
      // Stop here if the HTTP header isn't complete yet.
      if (req_str.find("\r\n\r\n"sv) == std::string_view::npos)
        return;
      // Dispatch to a handler or send an error if nothing matches.
      if (caf::starts_with(req_str, prom_request_start)) {
        BROKER_DEBUG("serve HTTP request for /metrics");
        on_metrics_request(msg.handle);
        return;
      }
      if (caf::starts_with(req_str, status_request_start)) {
        BROKER_DEBUG("serve HTTP request for /v1/status/json");
        on_status_request(msg.handle);
        return;
      }
      BROKER_DEBUG("reject unsupported HTTP request: "
                   << std::string{req_str.substr(0, req_str.find("\r\n"sv))});
      write(msg.handle, caf::as_bytes(caf::make_span(request_not_supported)));
      flush_and_close(msg.handle);
    },
    [this](const caf::io::new_connection_msg& msg) {
      // Pre-allocate buffer for maximum request size and start reading.
      requests_[msg.handle].buf.reserve(max_request_size);
      configure_read(msg.handle, caf::io::receive_policy::at_most(1024));
    },
    [this](const caf::io::connection_closed_msg& msg) {
      requests_.erase(msg.handle);
      if (num_connections() + num_doormen() == 0)
        quit();
    },
    [this](const caf::io::acceptor_closed_msg&) {
      BROKER_ERROR("Prometheus actor lost its acceptor!");
      if (num_connections() + num_doormen() == 0)
        quit();
    },
    [this](const data_message& msg) {
      BROKER_TRACE(BROKER_ARG(msg));
      collector_.insert_or_update(get_data(msg));
    },
    [this](atom::join, const filter_type& filter) {
      filter_ = filter;
      BROKER_INFO("collect remote metrics from topics" << filter_);
      send(core_, atom::join_v, filter_);
    },
  };
  auto params = metric_exporter_params::from(config());
  exporter_ = std::make_unique<exporter_state_type>(this, core_,
                                                    std::move(params));
  return bhvr.or_else(exporter_->make_behavior());
}

void prometheus_actor::flush_and_close(caf::io::connection_handle hdl) {
  flush(hdl);
  close(hdl);
  requests_.erase(hdl);
  if (num_connections() + num_doormen() == 0)
    quit();
}

void prometheus_actor::on_metrics_request(caf::io::connection_handle hdl) {
  // Collect metrics, ship response, and close. If the user configured
  // neither Broker-side import nor export of metrics, we fall back to the
  // default CAF Prometheus export.
  auto hdr = caf::as_bytes(caf::make_span(request_ok_text));
  BROKER_ASSERT(exporter_ != nullptr);
  if (!exporter_->running()) {
    exporter_->proc_importer.update();
    exporter_->impl.scrape(system().metrics());
  }
  collector_.insert_or_update(exporter_->impl.rows());
  auto text = collector_.prometheus_text();
  auto payload = caf::as_bytes(caf::make_span(text));
  auto& dst = wr_buf(hdl);
  dst.insert(dst.end(), hdr.begin(), hdr.end());
  dst.insert(dst.end(), payload.begin(), payload.end());
  flush_and_close(hdl);
}

void prometheus_actor::on_status_request(caf::io::connection_handle hdl) {
  auto aid = new_u64_id();
  request(core_, 5s, atom::get_v, atom::status_v)
    .then(
      [this, hdl, aid](const table& tbl) { //
        on_status_request_cb(hdl, aid, tbl);
      },
      [this, hdl, aid](const caf::error& what) {
        table tbl;
        tbl.emplace(data{"error"s}, data{caf::to_string(what)});
        on_status_request_cb(hdl, aid, tbl);
      });
  requests_[hdl].async_id = aid;
}

namespace {

class jsonizer {
public:
  using char_buf = std::vector<char>;

  explicit jsonizer(char_buf& out) : out_(out) {}

  template <class T>
  void operator()(const T& x) {
    add("null"sv);
  }

  void operator()(const boolean& x) {
    if (x)
      add("true"sv);
    else
      add("false"sv);
  }

  void operator()(const count& x) {
    add(std::to_string(x));
  }

  void operator()(const integer& x) {
    add(std::to_string(x));
  }

  void operator()(const real& x) {
    add(std::to_string(x));
  }

  void operator()(const std::string& x) {
    add('"');
    for (auto ch : x) {
      switch (ch) {
        case '"':
          add(R"(\")");
          break;
        default:
          add(ch);
      }
    }
    add('"');
  }

  void operator()(const set& xs) {
    apply_sequence(xs);
  }

  void operator()(const table& xs) {
    // Short-circuit empty tables.
    if (xs.empty()) {
      add("{}"sv);
      return;
    }
    // We only support tables with string keys. Map everything else to 'null'.
    auto has_string_key = [](const auto& kvp) {
      return is<std::string>(kvp.first);
    };
    if (!std::all_of(xs.begin(), xs.end(), has_string_key)) {
      add(R"_("<non-dict-table>")_"sv);
      return;
    }
    // Iterate through the table, mapping it a JSON object.
    auto i = xs.begin();
    block_open('{');
    apply_kvp(*i++);
    while (i != xs.end()) {
      add_sep();
      apply_kvp(*i++);
    }
    block_close('}');
  }

  void operator()(const vector& xs) {
    apply_sequence(xs);
  }

  template <class Container>
  void apply_sequence(const Container& xs) {
    // Short-circuit empty containers.
    if (xs.empty()) {
      add("[]"sv);
      return;
    }
    // Iterate through the container, mapping it a JSON array.
    auto i = xs.begin();
    block_open('[');
    std::visit(*this, (*i++).get_data());
    while (i != xs.end()) {
      add_sep();
      std::visit(*this, (*i++).get_data());
    }
    block_close(']');
  }

  template <class KeyValuePair>
  void apply_kvp(const KeyValuePair& kvp) {
    (*this)(get<std::string>(kvp.first));
    add(": "sv);
    std::visit(*this, kvp.second.get_data());
  }

private:
  void add(std::string_view str) {
    out_.insert(out_.end(), str.begin(), str.end());
  }

  void add(char ch) {
    out_.push_back(ch);
  }

  void add_nl() {
    add('\n');
    out_.insert(out_.end(), indent_, ' ');
  }

  void add_sep() {
    add(',');
    add_nl();
  }

  void block_open(char ch) {
    add(ch);
    indent_ += 2;
    add_nl();
  }

  void block_close(char ch) {
    indent_ -= 2;
    add_nl();
    add(ch);
  }

  char_buf& out_;
  size_t indent_ = 0;
};

} // namespace

void prometheus_actor::on_status_request_cb(caf::io::connection_handle hdl,
                                            uint64_t async_id,
                                            const table& res) {
  // Sanity checking.
  auto iter = requests_.find(hdl);
  if (iter == requests_.end())
    return;
  auto& req = iter->second;
  if (req.async_id != async_id)
    return;
  // Generate JSON output.
  json_buf_.clear();
  jsonizer f{json_buf_};
  f(res);
  json_buf_.push_back('\n');
  // Send result and close connection.
  auto hdr = caf::as_bytes(caf::make_span(request_ok_json));
  auto payload = caf::as_bytes(caf::make_span(json_buf_));
  auto& dst = wr_buf(hdl);
  dst.insert(dst.end(), hdr.begin(), hdr.end());
  dst.insert(dst.end(), payload.begin(), payload.end());
  flush_and_close(hdl);
}

} // namespace broker::internal
