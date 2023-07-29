#include "broker/internal/prometheus.hh"

#include <memory>
#include <string_view>

#include <caf/actor_system_config.hpp>
#include <caf/string_algorithms.hpp>

#include "broker/internal/logger.hh"
#include "broker/internal/type_id.hh"
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
  : super(cfg), core_(std::move(core)), exporter_(system()) {
  add_doorman(std::move(ptr));
}

// -- overrides ----------------------------------------------------------------

void prometheus_actor::on_exit() {
  requests_.clear();
  core_ = nullptr;
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
  if (auto imports = caf::get_or(config(), "broker.metrics.import.topics",
                                 std::vector<std::string>{});
      !imports.empty()) {
    for (auto& str : imports)
      filter_.emplace_back(std::move(str));
  }
  if (!filter_.empty()) {
    BROKER_INFO("collect remote metrics from topics" << filter_);
    send(core_, atom::join_v, filter_);
  }
  exporter_.schedule_first_tick(this);
  return {
    [this](caf::tick_atom) { exporter_.on_tick(this, core_); },
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
        exporter_.proc_importer.update();
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
    [this](data_message& msg) {
      BROKER_TRACE(BROKER_ARG(msg));
      auto&& rows_data = move_data(msg);
      if (!is<vector>(rows_data))
        return;
      using string = std::string;
      auto& rows = get<vector>(rows_data);
      if (rows.size() != 3 || !is<string>(rows[0]) || !is<timestamp>(rows[1])
          || !is<string>(rows[2]))
        return;
      remote_metrics_.insert_or_assign(std::move(get<string>(rows[0])),
                                       std::move(get<string>(rows[2])));
    },
    [this](atom::join, const filter_type& filter) {
      filter_ = filter;
      BROKER_INFO("collect remote metrics from topics" << filter_);
      send(core_, atom::join_v, filter_);
    },
  };
}

void prometheus_actor::flush_and_close(caf::io::connection_handle hdl) {
  flush(hdl);
  close(hdl);
  requests_.erase(hdl);
  if (num_connections() + num_doormen() == 0)
    quit();
}

namespace {

// Constructing a view from iterators is a C++20 feature.
caf::string_view to_sv(caf::string_view::iterator from,
                       caf::string_view::iterator to) {
  return caf::string_view{std::addressof(*from),
                          static_cast<size_t>(to - from)};
}

} // namespace

caf::string_view::iterator //
prometheus_actor::merge_metrics(const std::string& endpoint_name,
                                caf::string_view metric_name,
                                std::vector<char>& lines,
                                caf::string_view::iterator pos,
                                caf::string_view::iterator end) {
  auto append = [&lines](caf::string_view sv) {
    lines.insert(lines.end(), sv.begin(), sv.end());
  };
  // Iterate over the input.
  while (pos != end) {
    switch (*pos) {
      case ' ':
      case '\n':
        // We simply ignore leading whitespace and empty lines.
        ++pos;
        break;
      case '#':
        // Probably the start of a new help text or comment. Parse upstream.
        return pos;
      default: {
        // Prometheus format is: <metric>[{<label>...}] <value> <timestamp>.
        // Since <metric> is the first token, we can simply search for the
        // first non-alphanumeric character to find the end of the metric.
        auto pred = [](char c) { return !std::isalnum(c) && c != '_'; };
        auto eol = std::find(pos, end, '\n');
        auto sep = std::find_if(pos, eol, pred);
        if (sep == eol) {
          // Should not happen, but we handle it gracefully.
          BROKER_ERROR("invalid Prometheus text: " << std::string({pos, eol}));
        } else {
          // `sep` is now at the end of the metric name. Check if we are still
          // within the metric group.
          auto found_name = to_sv(pos, sep);
          if (found_name != metric_name) {
            // We found a new metric group. Handle upstream.
            return pos;
          }
          // We found a metric with the expected name. Add it to the group and
          // insert the endpoint label right after it.
          append(found_name);
          append("{endpoint=\"");
          append(endpoint_name);
          if (*sep == ' ') {
            // We found a metric without labels. Hence, we can close the
            // label set immediately.
            append("\"} ");
          } else {
            // We found a metric with labels. Hence, we need to insert a comma
            // here to separate the endpoint label from the rest of the labels.
            append("\",");
          }
          // Add the remainder of the line verbatim.
          append(to_sv(sep + 1, eol));
          lines.push_back('\n');
        }
        pos = eol;
      }
    }
  }
  return end;
}

void prometheus_actor::merge_metrics(const std::string& endpoint_name,
                                     caf::string_view prom_txt) {
  auto pos = prom_txt.begin();
  auto end = prom_txt.end();
  while (pos != end) {
    switch (*pos) {
      case ' ':
      case '\n':
        // We simply ignore leading whitespace and empty lines.
        ++pos;
        break;
      case '#': {
        // We found a comment. Ignore unless it's a HELP or TYPE line.
        auto eol = std::find(pos, end, '\n');
        auto line = to_sv(pos, eol);
        auto pred = [](char c) { return !std::isalnum(c) && c != '_'; };
        if (caf::starts_with(line, "# HELP ")) {
          // Chop off the prefix and parse the metric name.
          auto remainder = line.substr(8);
          auto sep = std::find_if(pos, eol, pred);
          auto metric_name = to_string(to_sv(pos, sep));
          // Add the helptext unless we have a prior definition.
          if (!metric_name.empty()) {
            auto& helptext = metric_groups_[metric_name].help;
            if (!helptext.empty())
              helptext.assign(pos, eol);
          }
        } else if (caf::starts_with(line, "# TYPE ")) {
          // Chop off the prefix and parse the metric name.
          auto remainder = line.substr(8);
          auto sep = std::find_if(pos, eol, pred);
          auto metric_name = to_string(to_sv(pos, sep));
          // Add the type annotation unless we have a prior definition.
          if (!metric_name.empty()) {
            auto& type= metric_groups_[metric_name].type;
            if (!type.empty())
              type.assign(pos, eol);
          }
        }
        pos = eol;
        break;
      }
      default: {
        // Prometheus format is: <metric>[{<label>...}] <value> <timestamp>.
        // Since <metric> is the first token, we can simply search for the
        // first non-alphanumeric character to find the end of the metric.
        auto pred = [](char c) { return !std::isalnum(c) && c != '_'; };
        auto eol = std::find(pos, end, '\n');
        auto sep = std::find_if(pos, eol, pred);
        auto metric_name = to_string(to_sv(pos, sep));
        auto& metric_lines = metric_groups_[metric_name].lines;
        if (metric_lines.empty())
          metric_lines.reserve(256);
        pos = merge_metrics(endpoint_name, metric_name, metric_lines, pos, end);
      }
    }
  }
}

void prometheus_actor::on_metrics_request(caf::io::connection_handle hdl) {
  // The HTTP header for the response.
  auto hdr = caf::as_bytes(caf::make_span(request_ok_text));
  // Collect metrics, ship response, and close. If the user configured
  // neither Broker-side import nor export of metrics, we fall back to the
  // default CAF Prometheus export.
  if (exporter_.name.empty()) {
    auto res = exporter_.collector.collect_from(system().metrics());
    auto res_bytes = caf::as_bytes(caf::make_span(res));
    auto& dst = wr_buf(hdl);
    dst.insert(dst.end(), hdr.begin(), hdr.end());
    dst.insert(dst.end(), res_bytes.begin(), res_bytes.end());
    flush_and_close(hdl);
    return;
  }
  // Otherwise, we need to add the endpoint as additional label dimension to all
  // metrics and ship them to the remote endpoint. For this, we parse the
  // Prometheus text output from local and remote endpoints and merge them.
  metric_groups_.clear();
  merge_metrics(exporter_.name,
                exporter_.collector.collect_from(system().metrics()));
  for (auto& [remote_name, prom_txt] : remote_metrics_) {
    merge_metrics(remote_name, prom_txt);
  }
  // Finally, we can ship the merged metrics to the remote endpoint.
  auto& dst = wr_buf(hdl);
  dst.insert(dst.end(), hdr.begin(), hdr.end());
  auto append_text = [&dst](caf::string_view str) {
    auto bytes = caf::as_bytes(caf::make_span(str));
    dst.insert(dst.end(), bytes.begin(), bytes.end());
  };
  for (auto& [metric_name, group] : metric_groups_) {
    auto& [type, help, lines] = group;
    if (lines.empty())
      continue;
    if (!type.empty()) {
      append_text(type);
      dst.push_back(caf::byte{'\n'});
    }
    if (!help.empty()) {
      append_text(help);
      dst.push_back(caf::byte{'\n'});
    }
    append_text({lines.data(), lines.size()});
    if (lines.back() != '\n')
      dst.push_back(caf::byte{'\n'});
  }
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
  buf_.clear();
  jsonizer f{buf_};
  f(res);
  buf_.push_back('\n');
  // Send result and close connection.
  auto hdr = caf::as_bytes(caf::make_span(request_ok_json));
  auto payload = caf::as_bytes(caf::make_span(buf_));
  auto& dst = wr_buf(hdl);
  dst.insert(dst.end(), hdr.begin(), hdr.end());
  dst.insert(dst.end(), payload.begin(), payload.end());
  flush_and_close(hdl);
}

} // namespace broker::internal
