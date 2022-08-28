#include "broker/internal/prometheus.hh"

#include <string_view>

#include <caf/actor_system_config.hpp>
#include <caf/attach_stream_sink.hpp>
#include <caf/string_algorithms.hpp>

#include "broker/internal/logger.hh"
#include "broker/internal/metric_exporter.hh"
#include "broker/message.hh"

namespace broker::internal {

namespace {

using std::string_view;

// Cap incoming HTTP requests.
constexpr size_t max_request_size = 512ul * 1024ul;

constexpr string_view valid_request_start = "GET /metrics HTTP/1.";

// HTTP response for requests that exceed the size limit.
constexpr string_view request_too_large =
  "HTTP/1.1 413 Request Entity Too Large\r\n"
  "Connection: Closed\r\n\r\n";

// HTTP response for requests that don't start with "GET /metrics HTTP/1".
constexpr string_view request_not_supported = "HTTP/1.1 501 Not Implemented\r\n"
                                              "Connection: Closed\r\n\r\n";

// HTTP header when sending a payload.
constexpr string_view request_ok = "HTTP/1.1 200 OK\r\n"
                                   "Content-Type: text/plain\r\n"
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
      auto flush_and_close = [this, &msg] {
        flush(msg.handle);
        close(msg.handle);
        requests_.erase(msg.handle);
        if (num_connections() + num_doormen() == 0)
          quit();
      };
      auto& req = requests_[msg.handle];
      if (req.size() + msg.buf.size() > max_request_size) {
        write(msg.handle, caf::as_bytes(caf::make_span(request_too_large)));
        flush_and_close();
        return;
      }
      req.insert(req.end(), msg.buf.begin(), msg.buf.end());
      auto req_str = string_view{reinterpret_cast<char*>(req.data()),
                                 req.size()};
      // Stop here if the first header line isn't complete yet.
      if (req_str.size() < valid_request_start.size())
        return;
      // We only check whether it's a GET request for /metrics for HTTP 1.x.
      // Everything else, we ignore for now.
      if (!caf::starts_with(req_str, valid_request_start)) {
        write(msg.handle, caf::as_bytes(caf::make_span(request_not_supported)));
        flush_and_close();
        return;
      }
      // Collect metrics, ship response, and close. If the user configured
      // neither Broker-side import nor export of metrics, we fall back to the
      // default CAF Prometheus export.
      auto hdr = caf::as_bytes(caf::make_span(request_ok));
      BROKER_ASSERT(exporter_ != nullptr);
      if (!exporter_->running()) {
        exporter_->proc_importer.update();
        exporter_->impl.scrape(system().metrics());
      }
      collector_.insert_or_update(exporter_->impl.rows());
      auto text = collector_.prometheus_text();
      auto payload = caf::as_bytes(caf::make_span(text));
      auto& dst = wr_buf(msg.handle);
      dst.insert(dst.end(), hdr.begin(), hdr.end());
      dst.insert(dst.end(), payload.begin(), payload.end());
      flush_and_close();
    },
    [this](const caf::io::new_connection_msg& msg) {
      // Pre-allocate buffer for maximum request size and start reading.
      requests_[msg.handle].reserve(max_request_size);
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

} // namespace broker::internal
