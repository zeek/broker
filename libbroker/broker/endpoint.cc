#include "broker/endpoint.hh"

#include "broker/configuration.hh"
#include "broker/defaults.hh"
#include "broker/detail/die.hh"
#include "broker/detail/filesystem.hh"
#include "broker/internal/configuration_access.hh"
#include "broker/internal/core_actor.hh"
#include "broker/internal/endpoint_access.hh"
#include "broker/internal/json_client.hh"
#include "broker/internal/json_type_mapper.hh"
#include "broker/internal/type_id.hh"
#include "broker/internal/web_socket.hh"
#include "broker/logger.hh"
#include "broker/port.hh"
#include "broker/publisher.hh"
#include "broker/status_subscriber.hh"
#include "broker/subscriber.hh"
#include "broker/timeout.hh"
#include "broker/zeek.hh"

#include <caf/actor.hpp>
#include <caf/actor_system.hpp>
#include <caf/actor_system_config.hpp>
#include <caf/config.hpp>
#include <caf/cow_string.hpp>
#include <caf/error.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/exit_reason.hpp>
#include <caf/flow/observable.hpp>
#include <caf/io/network/default_multiplexer.hpp>
#include <caf/message.hpp>
#include <caf/net/middleman.hpp>
#include <caf/net/tcp_stream_socket.hpp>
#include <caf/net/web_socket/server.hpp>
#include <caf/node_id.hpp>
#include <caf/scheduled_actor/flow.hpp>
#include <caf/scheduler/test_coordinator.hpp>
#include <caf/scoped_actor.hpp>
#include <caf/send.hpp>

#include "broker/defaults.hh"
#include "broker/detail/die.hh"
#include "broker/detail/filesystem.hh"
#include "broker/domain_options.hh"
#include "broker/endpoint.hh"
#include "broker/fwd.hh"
#include "broker/internal/connector.hh"
#include "broker/internal/core_actor.hh"
#include "broker/publisher.hh"
#include "broker/status_subscriber.hh"
#include "broker/subscriber.hh"
#include "broker/timeout.hh"

#include <chrono>
#include <memory>
#include <thread>

#include <prometheus/exposer.h>
#include <prometheus/registry.h>

#ifdef BROKER_WINDOWS
#  include "Winsock2.h"
#endif

using namespace std::literals;

namespace atom = broker::internal::atom;

using broker::internal::facade;
using broker::internal::native;

namespace broker {

// --- helper actors -----------------------------------------------------------

namespace {

template <class OnValue, class OnError>
struct async_helper_state {
  static inline const char* name = "broker.async-helper";

  caf::event_based_actor* self;
  caf::actor core;
  caf::message msg;
  OnValue on_value;
  OnError on_error;

  async_helper_state(caf::event_based_actor* self, caf::actor core,
                     caf::message msg, OnValue on_value, OnError on_error)
    : self(self),
      core(std::move(core)),
      msg(std::move(msg)),
      on_value(std::move(on_value)),
      on_error(std::move(on_error)) {
    // nop
  }

  caf::behavior make_behavior() {
    self->request(core, caf::infinite, std::move(msg))
      .then(std::move(on_value), std::move(on_error));
    return {};
  }
};

template <class OnValue, class OnError>
using async_helper_actor =
  caf::stateful_actor<async_helper_state<OnValue, OnError>>;

caf::actor_system_config& nat_cfg(configuration& cfg) {
  internal::configuration_access helper{&cfg};
  return helper.cfg();
}

} // namespace

// --- nested classes ----------------------------------------------------------

endpoint::clock::clock(internal::endpoint_context* ctx) : ctx_(ctx) {
  // nop
}

endpoint::clock::~clock() {
  // nop
}

class real_time_clock : public endpoint::clock {
public:
  using super = endpoint::clock;

  explicit real_time_clock(internal::endpoint_context* ctx) : super(ctx) {
    // nop
  }

  timestamp now() const noexcept override {
    return broker::now();
  }

  bool real_time() const noexcept override {
    return true;
  }

  void advance_time(timestamp) override {
    // nop
  }

  void send_later(worker dest, timespan after, void* vptr) override {
    auto& msg = *reinterpret_cast<caf::message*>(vptr);
    auto& sc = ctx_->sys.clock();
    auto t = sc.now() + after;
    auto me = caf::make_mailbox_element(nullptr, caf::make_message_id(),
                                        caf::no_stages, std::move(msg));
    sc.schedule_message(t, caf::actor_cast<caf::strong_actor_ptr>(native(dest)),
                        std::move(me));
  }
};

class sim_clock : public endpoint::clock {
public:
  using super = endpoint::clock;

  using mutex_type = std::mutex;

  using lock_type = std::unique_lock<mutex_type>;

  using pending_msg_type = std::pair<caf::actor, caf::message>;

  using pending_msgs_map_type = std::multimap<timestamp, pending_msg_type>;

  sim_clock(internal::endpoint_context* ctx)
    : super(ctx), time_since_epoch_(timespan{0}), pending_count_(0) {
    // nop
  }

  timestamp now() const noexcept override {
    return timestamp{time_since_epoch_.load()};
  }

  bool real_time() const noexcept override {
    return false;
  }

  void advance_time(timestamp t) override {
    // Advance time.
    if (t <= timestamp{time_since_epoch_})
      return;
    time_since_epoch_ = t.time_since_epoch();
    // Critical section: deliver messages.
    if (pending_count_ == 0)
      return;
    std::unordered_set<caf::actor> sync_with_actors;
    {
      lock_type guard{mtx_};
      auto it = pending_.begin();
      if (it->first > t)
        return;
      // Note: this function is performance-sensitive in the case of Zeek
      // reading pcaps and it's important to not construct fill the set unless
      // it's actually going to be used.
      while (it != pending_.end() && it->first <= t) {
        auto& pm = it->second;
        caf::anon_send(pm.first, std::move(pm.second));
        sync_with_actors.emplace(pm.first);
        it = pending_.erase(it);
        --pending_count_;
      }
    }
    // Send messages to all actors that we sync with.
    caf::scoped_actor self{ctx_->sys};
    for (auto& who : sync_with_actors)
      self->send(who, atom::sync_point_v, self);
    // Schedule a timeout (tick) message to abort syncing in case the actors
    // take too long.
    auto tout_mme = caf::make_mailbox_element(self->ctrl(),
                                              caf::make_message_id(),
                                              caf::no_stages, atom::tick_v);
    auto& caf_clock = self->clock();
    auto tout =
      caf_clock.schedule_message(caf_clock.now() + timeout::frontend,
                                 caf::actor_cast<caf::strong_actor_ptr>(self),
                                 std::move(tout_mme));
    // Wait for response messages.
    bool abort_syncing = false;
    for (size_t i = 0; !abort_syncing && i < sync_with_actors.size(); ++i)
      self->receive(
        [&](atom::sync_point) {
          // nop
        },
        [&](atom::tick) {
          log::endpoint::debug("sim-clock-timeout",
                               "advance_time actor syncing timed out");
          abort_syncing = true;
        },
        [&](const caf::error& reason) {
          log::endpoint::debug("sim-clock-error",
                               "advance_time actor syncing failed: {}", reason);
          abort_syncing = true;
        });
    // Dispose the timeout if it's still pending to avoid unnecessary messaging.
    if (!abort_syncing)
      tout.dispose();
  }

  void send_later(worker dest, timespan after, void* vptr) override {
    auto& msg = *reinterpret_cast<caf::message*>(vptr);
    lock_type guard{mtx_};
    auto t = this->now() + after;
    pending_.emplace(t,
                     pending_msg_type{std::move(native(dest)), std::move(msg)});
    ++pending_count_;
  }

private:
  /// Nanoseconds since start of the epoch.
  std::atomic<timespan> time_since_epoch_;

  /// Guards pending_.
  mutex_type mtx_;

  /// Stores pending messages until they time out.
  pending_msgs_map_type pending_;

  /// Stores number of items in pending_.  We track it separately as
  /// a micro-optimization -- checking pending_.size() would require
  /// obtaining a lock for mtx_, but instead checking this atomic avoids
  /// that locking expense in the common case.
  std::atomic<size_t> pending_count_;
};

endpoint::background_task::~background_task() {
  // nop
}

// --- endpoint class ----------------------------------------------------------

namespace {

struct connector_task : public endpoint::background_task {
public:
  connector_task() = default;
  connector_task(connector_task&&) = default;
  connector_task& operator=(connector_task&&) = default;

  connector_task(const connector_task&) = delete;
  connector_task& operator=(const connector_task&) = delete;

  ~connector_task() override {
    if (connector_) {
      connector_->async_shutdown();
      thread_.join();
    }
  }

  internal::connector_ptr start(caf::actor_system& sys, endpoint_id this_peer,
                                const broker_options& broker_cfg,
                                openssl_options_ptr ssl_cfg) {
    connector_ = std::make_shared<internal::connector>(this_peer, broker_cfg,
                                                       std::move(ssl_cfg));
    thread_ = std::thread{[ptr{connector_}, sys_ptr{&sys}] {
      CAF_SET_LOGGER_SYS(sys_ptr);
      ptr->run();
    }};
    return connector_;
  }

private:
  std::shared_ptr<internal::connector> connector_;
  std::thread thread_;
};

} // namespace

// --- endpoint class ----------------------------------------------------------

namespace {

struct indentation {
  size_t size;
};

indentation operator+(indentation x, size_t y) noexcept {
  return {x.size + y};
}

std::ostream& operator<<(std::ostream& out, indentation indent) {
  for (size_t i = 0; i < indent.size; ++i)
    out.put(' ');
  return out;
}

// TODO: this function replicates code in CAF for --dump-config; consider making
//       it a public API function in CAF.
void pretty_print(std::ostream& out, const caf::settings& xs,
                  indentation indent) {
  using std::cout;
  for (const auto& kvp : xs) {
    if (kvp.first == "dump-config")
      continue;
    if (auto submap = caf::get_if<caf::config_value::dictionary>(&kvp.second)) {
      out << indent << kvp.first << " {\n";
      pretty_print(out, *submap, indent + 2);
      out << indent << "}\n";
    } else if (auto lst = caf::get_if<caf::config_value::list>(&kvp.second)) {
      if (lst->empty()) {
        out << indent << kvp.first << " = []\n";
      } else {
        out << indent << kvp.first << " = [\n";
        auto list_indent = indent + 2;
        for (auto& x : *lst)
          out << list_indent << to_string(x) << ",\n";
        out << indent << "]\n";
      }
    } else {
      out << indent << kvp.first << " = " << to_string(kvp.second) << '\n';
    }
  }
}

} // namespace

endpoint::endpoint() : endpoint(configuration{}, endpoint_id::random()) {
  // nop
}

endpoint::endpoint(configuration config, prometheus_registry_ptr registry)
  : endpoint(std::move(config), endpoint_id::random(), std::move(registry)) {
  // nop
}

endpoint::endpoint(configuration config, endpoint_id id,
                   prometheus_registry_ptr registry)
  : id_(id), registry_(std::move(registry)) {
  if (registry_ == nullptr)
    registry_ = std::make_shared<prometheus::Registry>();
  // Spin up the actor system.
  auto broker_cfg = config.options();
  auto ssl_cfg = config.openssl_options();
  ctx_ = std::make_shared<internal::endpoint_context>(std::move(config));
  auto& sys = ctx_->sys;
  auto& cfg = nat_cfg(ctx_->cfg);
  // Stop immediately if any helptext was printed.
  if (cfg.cli_helptext_printed)
    exit(0);
  // Make sure the OpenSSL config is consistent.
  if (ssl_cfg && ssl_cfg->authentication_enabled()) {
    if (ssl_cfg->certificate.empty()) {
      std::cerr << "FATAL: No certificate configured for SSL endpoint.\n";
      ::abort();
    }
    if (ssl_cfg->key.empty()) {
      std::cerr << "FATAL: No private key configured for SSL endpoint.\n";
      ::abort();
    }
  }
  // Create a directory for storing the meta data if requested.
  auto meta_dir = get_or(cfg, "broker.recording-directory",
                         caf::string_view{defaults::recording_directory});
  if (!meta_dir.empty()) {
    if (detail::is_directory(meta_dir))
      detail::remove_all(meta_dir);
    if (detail::mkdirs(meta_dir)) {
      auto dump = cfg.dump_content();
      auto path = meta_dir + "/broker.conf";
      std::ofstream conf_file{path};
      if (!conf_file)
        log::endpoint::warning("recording-file-error",
                               "failed to write to config file {}", path);
      else
        pretty_print(conf_file, dump, {0});
    } else {
      log::endpoint::error("recording-file-error",
                           "unable to create '{}' for recording meta data",
                           meta_dir);
    }
  }
  // Spin up the connector unless disabled via config.
  internal::connector_ptr conn_ptr;
  if (!caf::get_or(cfg, "broker.disable-connector", false)) {
    auto conn_task = std::make_unique<connector_task>();
    conn_ptr = conn_task->start(sys, id_, broker_cfg, ssl_cfg);
    background_tasks_.emplace_back(std::move(conn_task));
  } else {
    log::endpoint::debug("no-connector",
                         "run without a connector (assuming test mode)");
  }
  // Initialize remaining state.
  auto opts = ctx_->cfg.options();
  if (opts.use_real_time)
    clock_ = std::make_unique<real_time_clock>(ctx_.get());
  else
    clock_ = std::make_unique<sim_clock>(ctx_.get());
  log::endpoint::info("start-endpoint", "creating endpoint {}");
  // TODO: the core actor may end up running basically nonstop in case it has a
  //       lot of incoming traffic to manage. CAF *should* suspend actors based
  //       on the 'caf.scheduler.max-throughput' setting. However, this is
  //       basically infinite (INT_MAX) by default and also currently doesn't
  //       work properly for flows. So until we find a good solution here, we
  //       spawn the core detached, because Zeek usually configures Broker to
  //       have a single thread for the CAF scheduler and we can end up starving
  //       background tasks. However, we must make sure to never detach the core
  //       when running the unit tests because we otherwise mess up the
  //       deterministic setup.
  caf::actor core;
  using core_t = internal::core_actor;
  domain_options adaptation{opts.disable_forwarding};
  if (auto sp = caf::get_as<std::string>(cfg, "caf.scheduler.policy");
      sp && *sp == "testing") {
    core = sys.spawn<core_t>(registry_, id_, filter_type{}, clock_.get(),
                             &adaptation, std::move(conn_ptr));
  } else {
    core = sys.spawn<core_t, caf::detached>(registry_, id_, filter_type{},
                                            clock_.get(), &adaptation,
                                            std::move(conn_ptr));
  }
  core_ = facade(core);
  // Spin up a Prometheus exposer if configured.
  if (auto port = caf::get_as<broker::port>(cfg, "broker.metrics.port")) {
    auto str = caf::get_or(cfg, "broker.metrics.address", ""s);
    if (!str.empty())
      str += ':';
    str += std::to_string(port->number());
    log::endpoint::info("metrics-exporter-started", "expose metrics on {}",
                        str);
    exposer_ = std::make_unique<prometheus::Exposer>(str);
    exposer_->RegisterCollectable(registry_);
  }
  // Spin up a WebSocket server when requested.
  if (auto port = caf::get_as<broker::port>(cfg, "broker.web-socket.port"))
    web_socket_listen(caf::get_or(cfg, "broker.web-socket.address", ""s),
                      port->number());
}

endpoint::~endpoint() {
  shutdown();
}

void endpoint::shutdown() {
  // Destroying a destroyed endpoint is a no-op.
  if (!ctx_)
    return;
  log::endpoint::info("shutdown-endpoint", "shutting down endpoint {}");
  if (!await_stores_on_shutdown_) {
    log::endpoint::debug("signal-store-shutdown",
                         "tell core actor to terminate stores");
    caf::anon_send(native(core_), atom::shutdown_v, atom::data_store_v);
  }
  // Lifetime scope of the scoped actor: must go out of scope before destroying
  // the actor system.
  {
    // TODO: there's got to be a better solution than calling the test
    //       coordinator manually here.
    using caf::scheduler::test_coordinator;
    auto& sys = ctx_->sys;
    auto sched = dynamic_cast<test_coordinator*>(&sys.scheduler());
    caf::scoped_actor self{sys};
    log::endpoint::debug("signal-core-shutdown", "tell core actor to stop");
    self->monitor(native(core_));
    self->send(native(core_), atom::shutdown_v, shutdown_options_);
    if (sched)
      sched->run();
    self->receive( // Give the core 5s time to shut down gracefully.
      [](const caf::down_msg&) {},
      caf::after(std::chrono::seconds(5)) >>
        [&] {
          log::endpoint::warning(
            "core-actor-signal-kill",
            "core actor failed to shut down gracefully, kill");
          self->send_exit(native(core_), caf::exit_reason::kill);
          self->wait_for(native(core_));
        });
    core_ = nullptr;
    log::endpoint::debug("stop-workers", "stop all background workers");
    if (!workers_.empty()) {
      for (auto& hdl : workers_)
        caf::anon_send_exit(native(hdl), caf::exit_reason::user_shutdown);
      log::endpoint::debug("wait-for-workers",
                           "wait until all background workers terminated");
      if (sched)
        sched->run();
      for (auto& hdl : workers_)
        self->wait_for(native(hdl));
      workers_.clear();
    }
  }
  log::endpoint::debug("stop-tasks", "stop {} background tasks",
                       background_tasks_.size());
  background_tasks_.clear();
  ctx_.reset();
  clock_.reset();
}

uint16_t endpoint::listen(const std::string& address, uint16_t port,
                          error* err_ptr, bool reuse_addr) {
  log::endpoint::info("try-listen", "try listening on {}:{} ({})", address,
                      port,
                      ctx_->cfg.options().disable_ssl ? "(no SSL)" : "(SSL)");
  uint16_t result = 0;
  caf::scoped_actor self{ctx_->sys};
  self
    ->request(native(core_), caf::infinite, atom::listen_v, address, port,
              reuse_addr)
    .receive(
      [&](atom::listen, atom::ok, uint16_t res) {
        BROKER_ASSERT(res != 0);
        log::endpoint::info("listen-success", "now listening on port {}", res);
        result = res;
      },
      [&](caf::error& err) {
        log::endpoint::warning("listen-failed",
                               "failed to listen on port {}: {}", port, err);
        if (err_ptr)
          *err_ptr = facade(err);
      });
  return result;
}

bool endpoint::peer(const std::string& address, uint16_t port,
                    timeout::seconds retry) {
  log::endpoint::info("sync-peer-start",
                      "starting to peer with {}:{} (retry: {}s) [synchronous]",
                      address, port, retry.count());
  bool result = false;
  caf::scoped_actor self{ctx_->sys};
  self
    ->request(native(core_), caf::infinite, atom::peer_v,
              network_info{address, port, retry})
    .receive(
      [&](atom::peer, atom::ok, endpoint_id id) {
        log::endpoint::info("sync-peer-success", "now peered to {}:{}, id: {}",
                            address, port, id);
        result = true;
      },
      [&](const caf::error& reason) {
        log::endpoint::warning("sync-peer-error", "cannot peer to {}:{}: {}",
                               address, port, reason);
      });
  return result;
}

void endpoint::peer_nosync(const std::string& address, uint16_t port,
                           timeout::seconds retry) {
  log::endpoint::info("async-peer-start",
                      "starting to peer with {}:{} (retry: {}s) [asynchronous]",
                      address, port, retry.count());
  caf::anon_send(native(core_), atom::peer_v,
                 network_info{address, port, retry});
}

std::future<bool> endpoint::peer_async(std::string host, uint16_t port,
                                       timeout::seconds retry) {
  auto prom = std::make_shared<std::promise<bool>>();
  auto res = prom->get_future();
  auto on_val = [prom](atom::peer, atom::ok, endpoint_id) mutable {
    prom->set_value(true);
  };
  auto on_err = [prom](const caf::error&) { prom->set_value(false); };
  using actor_t = async_helper_actor<decltype(on_val), decltype(on_err)>;
  auto msg = caf::make_message(atom::peer_v,
                               network_info{std::move(host), port, retry});
  ctx_->sys.spawn<actor_t>(native(core_), std::move(msg), std::move(on_val),
                           std::move(on_err));
  return res;
}

bool endpoint::unpeer(const std::string& address, uint16_t port) {
  log::endpoint::info("sync-unpeer-start",
                      "stopping to peer with {}:{} [synchronous]", address,
                      port);
  bool result = false;
  caf::scoped_actor self{ctx_->sys};
  self
    ->request(native(core_), caf::infinite, atom::unpeer_v,
              network_info{address, port})
    .receive(
      [&] {
        log::endpoint::info("sync-unpeer-success", "stopped peering with {}:{}",
                            address, port);
        result = true;
      },
      [&](const caf::error& reason) {
        log::endpoint::info("sync-unpeer-error", "cannot unpeer from {}:{}: {}",
                            address, port, reason);
      });

  return result;
}

void endpoint::unpeer_nosync(const std::string& address, uint16_t port) {
  log::endpoint::info("async-unpeer-start",
                      "stopping to peer with {}:{} [asynchronous]", address,
                      port);
  caf::anon_send(native(core_), atom::unpeer_v, network_info{address, port});
}

std::vector<peer_info> endpoint::peers() const {
  std::vector<peer_info> result;
  caf::scoped_actor self{ctx_->sys};
  self->request(native(core_), caf::infinite, atom::get_v, atom::peer_v)
    .receive([&](std::vector<peer_info>& peers) { result = std::move(peers); },
             [](const caf::error& e) {
               detail::die("failed to get peers:", to_string(e));
             });
  return result;
}

uint16_t endpoint::web_socket_listen(const std::string& address, uint16_t port,
                                     error* err, bool reuse_addr) {
  auto on_connect = [sp = &ctx_->sys, id = id_, core = native(core_)](
                      const caf::settings& hdr,
                      internal::web_socket::connect_event_t& ev) {
    auto& [pull, push] = ev;
    auto user_agent = caf::get_or(hdr, "web-socket.fields.User-Agent", "null");
    auto addr =
      network_info{caf::get_or(hdr, "web-socket.remote-address", "unknown"),
                   caf::get_or(hdr, "web-socket.remote-port", uint16_t{0}), 0s};
    log::endpoint::info(
      "web-socket-connect",
      "new WebSocket client with address {} and user agent {}", addr,
      user_agent);
    using impl_t = internal::json_client_actor;
    sp->spawn<impl_t>(id, core, addr, std::move(pull), std::move(push));
  };
  auto ssl_cfg = ctx_->cfg.openssl_options();
  auto res = internal::web_socket::launch(ctx_->sys, ssl_cfg, address, port,
                                          reuse_addr, "/v1/messages/json",
                                          std::move(on_connect));
  if (res) {
    return *res;
  } else {
    if (err)
      *err = std::move(res.error());
    return 0;
  }
}

std::vector<topic> endpoint::peer_subscriptions() const {
  std::vector<topic> result;
  caf::scoped_actor self{ctx_->sys};
  self
    ->request(native(core_), caf::infinite, atom::get_v, atom::peer_v,
              atom::subscriptions_v)
    .receive([&](std::vector<topic>& ts) { result = std::move(ts); },
             [](const caf::error& e) {
               detail::die("failed to get peer subscriptions:", to_string(e));
             });
  return result;
}

void endpoint::forward(std::vector<topic> ts) {
  log::endpoint::info("forward", "forwarding topics {}", ts);
  caf::anon_send(native(core_), atom::subscribe_v, std::move(ts));
}

void endpoint::publish(topic t, const data& d) {
  publish(make_data_message(std::move(t), d));
}

void endpoint::publish(topic t, variant d) {
  publish(make_data_message(std::move(t), std::move(d)));
}

void endpoint::publish(std::string_view t, const zeek::Message& d) {
  publish(make_data_message(t, d.raw()));
}

void endpoint::publish(const endpoint_info& dst, topic t, const data& d) {
  publish(dst, make_data_message(std::move(t), d));
}

void endpoint::publish(const endpoint_info& dst, topic t, const variant& d) {
  publish(dst, make_data_message(std::move(t), d));
}

void endpoint::publish(const endpoint_info& dst, std::string_view t,
                       const zeek::Message& d) {
  publish(dst, make_data_message(t, d.raw()));
}

void endpoint::publish(data_message x) {
  log::endpoint::debug("publish", "publishing {} at {}", x->value(),
                       x->topic());
  caf::anon_send(native(core_), atom::publish_v, std::move(x));
}

void endpoint::publish(std::vector<data_message> xs) {
  for (auto& x : xs)
    publish(std::move(x));
}

void endpoint::publish(const endpoint_info& dst, data_message msg) {
  caf::anon_send(native(core_), atom::publish_v, std::move(msg), dst);
}

publisher endpoint::make_publisher(topic ts) {
  return publisher::make(*this, std::move(ts));
}

status_subscriber endpoint::make_status_subscriber(bool receive_statuses,
                                                   size_t queue_size) {
  return status_subscriber::make(*this, receive_statuses, queue_size);
}

subscriber endpoint::make_subscriber(filter_type filter, size_t queue_size) {
  return subscriber::make(*this, std::move(filter), queue_size);
}

namespace {

struct worker_state {
  static inline const char* name = "broker.subscriber";
};

using worker_actor = caf::stateful_actor<worker_state>;

} // namespace

worker endpoint::do_subscribe(filter_type&& filter,
                              const detail::sink_driver_ptr& sink) {
  BROKER_ASSERT(sink != nullptr);
  using caf::async::make_spsc_buffer_resource;
  // Get a pair of connected resources.
  // Note: structured bindings with values confuses clang-tidy's leak checker.
  auto resources = make_spsc_buffer_resource<data_message>();
  auto& [con_res, prod_res] = resources;
  // Subscribe a new worker to the consumer end.
  auto [obs, launch_obs] = ctx_->sys.spawn_inactive<worker_actor>();
  sink->init();
  obs //
    ->make_observable()
    .from_resource(con_res)
    .subscribe(caf::flow::make_observer(
      [sink](const data_message& msg) { sink->on_next(msg); },
      [sink](const caf::error& err) { sink->on_cleanup(facade(err)); },
      [sink] {
        error no_error;
        sink->on_cleanup(no_error);
      }));
  auto worker = caf::actor{obs};
  launch_obs();
  // Hand the producer end to the core.
  caf::anon_send(native(core()), std::move(filter), std::move(prod_res));
  // Store background worker and return.
  workers_.emplace_back(facade(worker));
  return workers_.back();
}

namespace {

// Implements the Pullable concept from CAF.
class data_message_source {
public:
  using driver_ptr = detail::source_driver_ptr;

  using output_type = data_message;

  explicit data_message_source(driver_ptr driver) : driver_(std::move(driver)) {
    // nop
  }

  data_message_source(data_message_source&&) = default;
  data_message_source(const data_message_source&) = default;
  data_message_source& operator=(data_message_source&&) = default;
  data_message_source& operator=(const data_message_source&) = default;

  template <class Step, class... Steps>
  void pull(size_t n, Step& step, Steps&... steps) {
    // Stop when already at the end.
    if (driver_->at_end()) {
      step.on_complete(steps...);
      return;
    }
    // Pull from the driver and propagate values down the pipeline.
    buf_.clear();
    driver_->pull(buf_, n);
    for (auto& msg : buf_)
      if (!step.on_next(msg, steps...))
        return;
    // Check for end condition again.
    if (driver_->at_end()) {
      step.on_complete(steps...);
      return;
    }
  }

private:
  driver_ptr driver_;
  std::deque<data_message> buf_;
};

} // namespace

worker
endpoint::do_publish_all(const std::shared_ptr<detail::source_driver>& driver) {
  BROKER_ASSERT(driver != nullptr);
  using caf::async::make_spsc_buffer_resource;
  // Get a pair of connected resources.
  // Note: structured bindings with values confuses clang-tidy's leak checker.
  auto resources = make_spsc_buffer_resource<data_message>();
  auto [con_res, prod_res] = resources;
  // Push to the producer end with a new worker.
  auto [src, launch_src] = ctx_->sys.spawn_inactive<worker_actor>();
  driver->init();
  src //
    ->make_observable()
    .from_generator(data_message_source{driver})
    .subscribe(prod_res);
  auto worker = caf::actor{src};
  launch_src();
  // Hand the consumer end to the core.
  caf::anon_send(native(core_),
                 internal::data_consumer_res{std::move(con_res)});
  // Store background worker and return.
  workers_.emplace_back(facade(worker));
  return workers_.back();
}

broker_options endpoint::options() const {
  return ctx_->cfg.options();
}

expected<store> endpoint::attach_master(std::string name, backend type,
                                        backend_options opts) {
  log::endpoint::info("attach-master", "attaching master store {} of type {}",
                      name, type);
  expected<store> res{ec::unspecified};
  caf::scoped_actor self{ctx_->sys};
  self
    ->request(native(core_), caf::infinite, atom::data_store_v, atom::master_v,
              atom::attach_v, name, type, std::move(opts))
    .receive(
      [&](caf::actor& master) {
        log::endpoint::info("attach-master-success",
                            "successfully attached master store {} of type {}",
                            name, type);
        res = store{id_, facade(master), std::move(name)};
      },
      [&](const caf::error& reason) {
        log::endpoint::info("attach-master-failed",
                            "failed to attached master store {} of type {}: {}",
                            name, type, reason);
        res = facade(reason);
      });
  return res;
}

expected<store> endpoint::attach_clone(std::string name, double resync_interval,
                                       double stale_interval,
                                       double mutation_buffer_interval) {
  log::endpoint::info("attach-clone",
                      "attaching clone store {}, resync-interval: {}, "
                      "stale-interval: {}, mutation-buffer-interval: {}",
                      name, resync_interval, stale_interval,
                      mutation_buffer_interval);
  expected<store> res{ec::unspecified};
  caf::scoped_actor self{ctx_->sys};
  self
    ->request(native(core_), caf::infinite, atom::data_store_v, atom::clone_v,
              atom::attach_v, name, resync_interval, stale_interval,
              mutation_buffer_interval)
    .receive(
      [&](caf::actor& clone) {
        log::endpoint::info("attach-clone-success",
                            "successfully attached clone store {}", name);
        res = store{id_, facade(clone), std::move(name)};
      },
      [&](const caf::error& reason) {
        log::endpoint::info("attach-clone-failed",
                            "failed to attached clone store {}: {}", name,
                            reason);
        res = facade(reason);
      });
  return res;
}

void endpoint::init_socket_api() {
#ifdef BROKER_WINDOWS
  WSADATA WinsockData;
  if (WSAStartup(MAKEWORD(2, 2), &WinsockData) != 0) {
    fprintf(stderr, "WSAStartup failed\n");
    abort();
  }
#endif
}

void endpoint::deinit_socket_api() {
#ifdef BROKER_WINDOWS
  WSACleanup();
#endif
}

void endpoint::init_system() {
  configuration::init_global_state();
  init_socket_api();
  init_ssl_api();
}

void endpoint::deinit_system() {
  deinit_ssl_api();
  deinit_socket_api();
}

bool endpoint::await_peer(endpoint_id whom, timespan timeout) {
  bool result = false;
  caf::scoped_actor self{ctx_->sys};
  self->request(native(core()), timeout, atom::await_v, whom)
    .receive(
      [&]([[maybe_unused]] endpoint_id& discovered) {
        BROKER_ASSERT(whom == discovered);
        result = true;
      },
      [&](caf::error& e) {
        // nop
      });
  return result;
}

void endpoint::await_peer(endpoint_id whom, std::function<void(bool)> callback,
                          timespan timeout) {
  if (!callback) {
    log::endpoint::error("invalid-callback",
                         "invalid callback received for await_peer");
    return;
  }
  auto f = [whom, cb{std::move(callback)}](caf::event_based_actor* self,
                                           const caf::actor& core, timespan t) {
    self->request(core, t, atom::await_v, whom)
      .then(
        [&]([[maybe_unused]] endpoint_id& discovered) {
          BROKER_ASSERT(whom == discovered);
          cb(true);
        },
        [&](caf::error& e) { cb(false); });
  };
  ctx_->sys.spawn(f, native(core_), timeout);
}

bool endpoint::await_filter_entry(const topic& what, timespan timeout) {
  using namespace std::literals;
  auto abs_timeout = broker::now() + timeout;
  for (;;) {
    auto xs = filter();
    if (std::find(xs.begin(), xs.end(), what) != xs.end()) {
      return true;
    } else if (broker::now() < abs_timeout) {
      std::this_thread::sleep_for(10ms);
    } else {
      return false;
    }
  }
}

filter_type endpoint::filter() const {
  filter_type result;
  caf::scoped_actor self{ctx_->sys};
  self->request(native(core()), caf::infinite, atom::get_filter_v)
    .receive(
      [&](filter_type& res) {
        using std::swap;
        swap(res, result);
      },
      [](caf::error&) {
        // nop
      });
  return result;
}

// -- worker management --------------------------------------------------------

void endpoint::wait_for(worker who) {
  caf::scoped_actor tmp{ctx_->sys};
  tmp->wait_for(native(who));
  if (auto i = std::find(workers_.begin(), workers_.end(), who);
      i != workers_.end()) {
    workers_.erase(i);
  }
}

void endpoint::stop(worker who) {
  caf::anon_send_exit(native(who), caf::exit_reason::user_shutdown);
  if (auto i = std::find(workers_.begin(), workers_.end(), who);
      i != workers_.end()) {
    workers_.erase(i);
  }
}

} // namespace broker

namespace broker::internal {

endpoint_context::endpoint_context(configuration&& src)
  : cfg(std::move(src)), sys(nat_cfg(cfg)) {
  // nop
}

caf::actor_system& endpoint_access::sys() {
  return ep->ctx_->sys;
}

const caf::actor_system_config& endpoint_access::cfg() {
  return nat_cfg(ep->ctx_->cfg);
}

std::shared_ptr<endpoint_context> endpoint_access::ctx() {
  return ep->ctx_;
}

} // namespace broker::internal
