#include "broker/endpoint.hh"

#include "broker/configuration.hh"
#include "broker/defaults.hh"
#include "broker/detail/die.hh"
#include "broker/detail/filesystem.hh"
#include "broker/internal/configuration_access.hh"
#include "broker/internal/core_actor.hh"
#include "broker/internal/endpoint_access.hh"
#include "broker/internal/logger.hh"
#include "broker/internal/metric_exporter.hh"
#include "broker/internal/prometheus.hh"
#include "broker/publisher.hh"
#include "broker/status_subscriber.hh"
#include "broker/subscriber.hh"
#include "broker/timeout.hh"

#include <caf/actor.hpp>
#include <caf/actor_system.hpp>
#include <caf/attach_stream_sink.hpp>
#include <caf/attach_stream_source.hpp>
#include <caf/config.hpp>
#include <caf/downstream.hpp>
#include <caf/error.hpp>
#include <caf/exit_reason.hpp>
#include <caf/io/middleman.hpp>
#include <caf/io/network/default_multiplexer.hpp>
#include <caf/io/network/multiplexer.hpp>
#include <caf/message.hpp>
#include <caf/node_id.hpp>
#include <caf/openssl/publish.hpp>
#include <caf/scoped_actor.hpp>
#include <caf/send.hpp>

#include <iostream>
#include <unordered_set>
#include <thread>

namespace atom = broker::internal::atom;

using broker::internal::facade;
using broker::internal::native;

namespace broker {

namespace {

caf::actor_system_config& nat_cfg(configuration& cfg) {
  internal::configuration_access helper{&cfg};
  return helper.cfg();
}

} // namespace

// --- nested classes ----------------------------------------------------------

struct endpoint::context {
  configuration cfg;
  caf::actor_system sys;

  context(configuration&& src) : cfg(std::move(src)), sys(nat_cfg(cfg)) {
    // nop
  }
};

endpoint::clock::clock(endpoint::context* ctx) : ctx_(ctx) {
  // nop
}

endpoint::clock::~clock() {
  // nop
}

class real_time_clock : public endpoint::clock {
public:
  using super = endpoint::clock;

  explicit real_time_clock(endpoint::context* ctx) : super(ctx) {
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

  sim_clock(endpoint::context* ctx)
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
    // Wait for response messages.
    caf::scoped_actor self{ctx_->sys};
    for (auto& who : sync_with_actors) {
      self->send(who, atom::sync_point_v, self);
      self->delayed_send(self, timeout::frontend, atom::tick_v);
      self->receive(
        [&](atom::sync_point) {
          // nop
        },
        [&](atom::tick) {
          BROKER_DEBUG("advance_time actor syncing timed out");
        },
        [&](caf::error& e) {
          BROKER_DEBUG("advance_time actor syncing failed");
        });
    }
  }

  void send_later(worker dest, timespan after, void* vptr) override{
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

namespace {

class prometheus_http_task : public endpoint::background_task {
public:
  prometheus_http_task(caf::actor_system& sys) : mpx_(&sys) {
    // nop
  }

  template <class T>
  bool has(caf::string_view name) {
    auto res = caf::get_as<T>(mpx_.system().config(), name);
    return static_cast<bool>(res);
  }

  expected<uint16_t> start(uint16_t port, caf::actor core,
                           const char* in = nullptr, bool reuse = false) {
    caf::io::doorman_ptr dptr;
    if (auto maybe_dptr = mpx_.new_tcp_doorman(port, in, reuse)) {
      dptr = std::move(*maybe_dptr);
    } else {
      return facade(maybe_dptr.error());
    }
    auto actual_port = dptr->port();
    using impl = internal::prometheus_actor;
    mpx_supervisor_ = mpx_.make_supervisor();
    caf::actor_config cfg{&mpx_};
    worker_ = mpx_.system().spawn_impl<impl, caf::hidden>(cfg, std::move(dptr),
                                                          std::move(core));
    struct { // TODO: replace with std::latch when available.
      std::mutex mx;
      std::condition_variable cv;
      bool lit = false;
      void ignite() {
        std::unique_lock<std::mutex> guard{mx};
        lit = true;
        cv.notify_all();
      }
      void wait() {
        std::unique_lock<std::mutex> guard{mx};
        while (!lit)
          cv.wait(guard);
      }
    } beacon;
    auto run_mpx = [this, &beacon] {
      CAF_LOG_TRACE("");
      mpx_.thread_id(std::this_thread::get_id());
      beacon.ignite();
      mpx_.run();
    };
    thread_ = mpx_.system().launch_thread("broker.prom", run_mpx);
    beacon.wait();
    return actual_port;
  }

  ~prometheus_http_task() {
    if (mpx_supervisor_) {
      mpx_.dispatch([=] {
        auto base_ptr = caf::actor_cast<caf::abstract_actor*>(worker_);
        auto ptr = static_cast<caf::io::broker*>(base_ptr);
        if (!ptr->getf(caf::abstract_actor::is_terminated_flag)) {
          ptr->context(&mpx_);
          ptr->quit();
          ptr->finalize();
        }
      });
      mpx_supervisor_.reset();
      thread_.join();
    }
  }

  caf::actor telemetry_exporter() {
    return worker_;
  }

private:
  caf::io::network::default_multiplexer mpx_;
  caf::io::network::multiplexer::supervisor_ptr mpx_supervisor_;
  caf::actor worker_;
  std::thread thread_;
};

} // namespace

// --- metrics_exporter_t::endpoint class --------------------------------------

void endpoint::metrics_exporter_t::set_interval(caf::timespan new_interval) {
  if (new_interval.count() > 0)
    caf::anon_send(native(parent_->telemetry_exporter_), atom::put_v,
                   new_interval);
}

void endpoint::metrics_exporter_t::set_target(topic new_target) {
  if (!new_target.empty())
    caf::anon_send(native(parent_->telemetry_exporter_), atom::put_v,
                   std::move(new_target));
}

void endpoint::metrics_exporter_t::set_id(std::string new_id) {
  if (!new_id.empty())
    caf::anon_send(native(parent_->telemetry_exporter_), atom::put_v,
                   std::move(new_id));
}

void endpoint::metrics_exporter_t::set_prefixes(
  std::vector<std::string> new_prefixes) {
  // We only wrap the prefixes into a filter to get around assigning a type ID
  // to std::vector<std::string> (which technically would require us to change
  // Broker ID on the network).
  filter_type boxed;
  for (auto& prefix : new_prefixes)
    boxed.emplace_back(std::move(prefix));
  caf::anon_send(native(parent_->telemetry_exporter_), atom::put_v,
                 std::move(boxed));
}

// --- endpoint class ----------------------------------------------------------

endpoint_id endpoint::node_id() const {
  return facade(ctx_->sys.node());
}

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

endpoint::endpoint() : endpoint(configuration{}) {
  // nop
}

endpoint::endpoint(configuration config) {
  ctx_.reset(new context(std::move(config)));
  auto& sys = ctx_->sys;
  auto& cfg = nat_cfg(ctx_->cfg);
  // Stop immediately if any helptext was printed.
  if (cfg.cli_helptext_printed)
    exit(0);
  // Create a directory for storing the meta data if requested.
  auto meta_dir = get_or(cfg, "broker.recording-directory",
                         caf::string_view{defaults::recording_directory});
  if (!meta_dir.empty()) {
    if (detail::is_directory(meta_dir))
      detail::remove_all(meta_dir);
    if (detail::mkdirs(meta_dir)) {
      auto dump = cfg.dump_content();
      std::ofstream conf_file{meta_dir + "/broker.conf"};
      if (!conf_file)
        BROKER_WARNING("failed to write to config file");
      else
        pretty_print(conf_file, dump, {0});
    } else {
      std::cerr << "WARNING: unable to create \"" << meta_dir
                << "\" for recording meta data\n";
    }
  }
  // Initialize remaining state.
  auto& opts = ctx_->cfg.options();
  if (opts.use_real_time)
    clock_.reset(new real_time_clock(ctx_.get()));
  else
    clock_.reset(new sim_clock(ctx_.get()));
  if (ctx_->sys.has_openssl_manager() || opts.disable_ssl) {
    BROKER_INFO("creating core actor");
    auto hdl
      = sys.spawn<internal::core_actor_type>(filter_type{}, opts, clock_.get());
    core_ = facade(hdl);
  } else {
    detail::die("SSL is enabled but CAF OpenSSL manager is not available");
  }
  // Spin up a Prometheus actor if configured or an exporter.
  if (auto port = caf::get_as<uint16_t>(cfg, "broker.metrics.port")) {
    auto ptask = std::make_unique<prometheus_http_task>(sys);
    auto addr = caf::get_or(cfg, "broker.metrics.address", std::string{});
    if (auto actual_port = ptask->start(
          *port, native(core_), addr.empty() ? nullptr : addr.c_str(), false)) {
      BROKER_INFO("expose metrics on port" << *actual_port);
      telemetry_exporter_ = facade(ptask->telemetry_exporter());
      background_tasks_.emplace_back(std::move(ptask));
    } else {
      BROKER_ERROR("failed to expose metrics:" << actual_port.error());
    }
  } else {
    using exporter_t = internal::metric_exporter_actor;
    auto params = internal::metric_exporter_params::from(cfg);
    auto hdl = sys.spawn<exporter_t>(native(core_), std::move(params));
    telemetry_exporter_ = facade(hdl);
  }
}

endpoint::~endpoint() {
  BROKER_INFO("destroying endpoint");
  shutdown();
}

void endpoint::shutdown() {
  BROKER_INFO("shutting down endpoint");
  if (destroyed_)
    return;
  destroyed_ = true;
  if (!await_stores_on_shutdown_) {
    BROKER_DEBUG("tell core actor to terminate stores");
    caf::anon_send(native(core_), atom::shutdown_v, atom::store_v);
  }
  if (!children_.empty()) {
    caf::scoped_actor self{ctx_->sys};
    BROKER_DEBUG("send exit messages to all children");
    for (auto& child : children_)
      // exit_reason::kill seems more reliable than
      // exit_reason::user_shutdown in terms of avoiding deadlocks/hangs,
      // possibly due to the former having more explicit logic that will
      // shut down streams.
      self->send_exit(native(child), caf::exit_reason::kill);
    BROKER_DEBUG("wait until all children have terminated");
    for (auto& child : children_)
      self->wait_for(native(child));
    children_.clear();
  }
  BROKER_DEBUG("stop background tasks");
  telemetry_exporter_ = nullptr;
  background_tasks_.clear();
  BROKER_DEBUG("send shutdown message to core actor");
  caf::anon_send(native(core_), atom::shutdown_v);
  core_ = nullptr;
  clock_.reset();
  ctx_.reset();
}

uint16_t endpoint::listen(const std::string& address, uint16_t port) {
  BROKER_INFO("listening on"
              << (address + ":" + std::to_string(port))
              << (ctx_->cfg.options().disable_ssl ? "(no SSL)" : "(SSL)"));
  char const* addr = address.empty() ? nullptr : address.c_str();
  uint16_t res = 0;
  auto set_res = [&res](const auto& pub_res) {
    if (pub_res)
      res = *pub_res;
    // else: use default
  };
  if (ctx_->cfg.options().disable_ssl)
    set_res(ctx_->sys.middleman().publish(native(core_), port, addr, true));
  else
    set_res(caf::openssl::publish(native(core_), port, addr, true));
  return res;
}

bool endpoint::peer(const std::string& address, uint16_t port,
                    timeout::seconds retry) {
  BROKER_TRACE(BROKER_ARG(address) << BROKER_ARG(port) << BROKER_ARG(retry));
  BROKER_INFO("starting to peer with" << (address + ":" + std::to_string(port))
                                      << "retry:" << to_string(retry)
                                      << "[synchronous]");
  bool result = false;
  caf::scoped_actor self{ctx_->sys};
  self
    ->request(native(core_), caf::infinite, atom::peer_v,
              network_info{address, port, retry})
    .receive([&](const caf::actor&) { result = true; },
             [&](caf::error& err) {
               BROKER_DEBUG("Cannot peer to" << address << "on port" << port
                                             << ":" << err);
             });
  return result;
}

void endpoint::peer_nosync(const std::string& address, uint16_t port,
			   timeout::seconds retry) {
  BROKER_TRACE(BROKER_ARG(address) << BROKER_ARG(port));
  BROKER_INFO("starting to peer with" << (address + ":" + std::to_string(port))
                                      << "retry:" << to_string(retry)
                                      << "[asynchronous]");
  caf::anon_send(native(core_), atom::peer_v,
                 network_info{address, port, retry});
}

bool endpoint::unpeer(const std::string& address, uint16_t port) {
  BROKER_TRACE(BROKER_ARG(address) << BROKER_ARG(port));
  BROKER_INFO("stopping to peer with" << address << ":" << port
                                      << "[synchronous]");
  bool result = false;
  caf::scoped_actor self{ctx_->sys};
  self
    ->request(native(core_), caf::infinite, atom::unpeer_v,
              network_info{address, port})
    .receive([&](void) { result = true; },
             [&](caf::error& err) {
               BROKER_DEBUG("Cannot unpeer from" << address << "on port" << port
                                                 << ":" << err);
             });

  return result;
}

void endpoint::unpeer_nosync(const std::string& address, uint16_t port) {
  BROKER_TRACE(BROKER_ARG(address) << BROKER_ARG(port));
  BROKER_INFO("stopping to peer with " << address << ":" << port
                                       << "[asynchronous]");
  caf::anon_send(native(core_), atom::unpeer_v, network_info{address, port});
}

std::vector<peer_info> endpoint::peers() const {
  std::vector<peer_info> result;
  caf::scoped_actor self{ctx_->sys};
  self->request(native(core_), caf::infinite, atom::get_v, atom::peer_v)
  .receive(
    [&](std::vector<peer_info>& peers) {
      result = std::move(peers);
    },
    [](const caf::error& e) {
      detail::die("failed to get peers:", to_string(e));
    }
  );
  return result;
}

std::vector<topic> endpoint::peer_subscriptions() const {
  std::vector<topic> result;
  caf::scoped_actor self{ctx_->sys};
  self->request(native(core_), caf::infinite, atom::get_v,
                atom::peer_v, atom::subscriptions_v)
  .receive(
    [&](std::vector<topic>& ts) {
      result = std::move(ts);
    },
    [](const caf::error& e) {
      detail::die("failed to get peer subscriptions:", to_string(e));
    }
  );
  return result;
}

void endpoint::forward(std::vector<topic> ts)
{
  BROKER_INFO("forwarding topics" << ts);
  caf::anon_send(native(core_), atom::subscribe_v, std::move(ts));
}

void endpoint::publish(topic t, data d) {
  BROKER_INFO("publishing" << std::make_pair(t, d));
  caf::anon_send(native(core_), atom::publish_v,
                 make_data_message(std::move(t), std::move(d)));
}

void endpoint::publish(const endpoint_info& dst, topic t, data d) {
  BROKER_INFO("publishing" << std::make_pair(t, d) << "to" << dst.node);
  caf::anon_send(native(core_), atom::publish_v, dst,
                 make_data_message(std::move(t), std::move(d)));
}

void endpoint::publish(data_message x){
  BROKER_INFO("publishing" << x);
  caf::anon_send(native(core_), atom::publish_v, std::move(x));
}


void endpoint::publish(std::vector<data_message> xs) {
  BROKER_INFO("publishing" << xs.size() << "messages");
  for (auto& x : xs)
    publish(std::move(x));
}

publisher endpoint::make_publisher(topic ts) {
  publisher result{*this, std::move(ts)};
  children_.emplace_back(result.worker());
  return result;
}

status_subscriber endpoint::make_status_subscriber(bool receive_statuses) {
  status_subscriber result{*this, receive_statuses};
  children_.emplace_back(result.worker());
  return result;
}

subscriber endpoint::make_subscriber(std::vector<topic> ts, size_t max_qsize) {
  subscriber result{*this, std::move(ts), max_qsize};
  children_.emplace_back(result.worker());
  return result;
}

template <class F>
worker endpoint::make_worker(F fn) {
  auto nat = ctx_->sys.spawn([f{std::move(fn)}](caf::event_based_actor* self) {
#ifndef CAF_NO_EXCEPTION
    // "Hide" unhandled-exception warning if users throw.
    self->set_exception_handler(
      [](caf::scheduled_actor* thisptr, std::exception_ptr& e) -> caf::error {
        return caf::sec::runtime_error;
      }
    );
#endif // CAF_NO_EXCEPTION
    // Run callback.
    f(self);
  });
  auto hdl = facade(nat);
  children_.emplace_back(hdl);
  return hdl;
}

broker_options endpoint::options() const {
  return ctx_->cfg.options();
}

worker endpoint::do_subscribe(std::vector<topic>&& topics,
                              std::shared_ptr<detail::sink_driver> driver,
                              bool block_until_initialized) {
  using caf::unit_t;
  using self_ptr = caf::event_based_actor*;
  if (block_until_initialized) {
    std::mutex mx;
    std::condition_variable cv;
    auto act = [driver, chdl{core()}, ts{std::move(topics)}, &mx, &cv](self_ptr self) {
      self->send(self * native(chdl), atom::join_v, std::move(ts));
      self->become([=](caf::stream<data_message> in) {
        caf::attach_stream_sink(
          self, in, [driver](unit_t&) { driver->init(); },
          [driver](unit_t&, data_message x) { driver->on_next(x); },
          [driver](unit_t&, const caf::error& err) {
            driver->on_cleanup(facade(err));
          });
        self->unbecome();
      });
      std::unique_lock<std::mutex> guard{mx};
      cv.notify_one();
    };
    auto res = make_worker(std::move(act));
    std::unique_lock<std::mutex> guard{mx};
    cv.wait(guard);
    return res;
  } else {
    auto act = [driver, chdl{core()}, ts{std::move(topics)}](self_ptr self) {
      self->send(self * native(chdl), atom::join_v, std::move(ts));
      self->become([=](caf::stream<data_message> in) {
        caf::attach_stream_sink(
          self, in, [driver](unit_t&) { driver->init(); },
          [driver](unit_t&, data_message x) { driver->on_next(x); },
          [driver](unit_t&, const caf::error& err) {
            driver->on_cleanup(facade(err));
          });
        self->unbecome();
      });
    };
    return make_worker(std::move(act));
  }
}

worker endpoint::do_publish_all(std::shared_ptr<detail::source_driver> driver,
                                bool block_until_initialized) {
  using caf::unit_t;
  using self_ptr = caf::event_based_actor*;
  if (block_until_initialized) {
    std::mutex mx;
    std::condition_variable cv;
    auto act = [driver, chdl{core()}, &mx, &cv](self_ptr self) {
      caf::attach_stream_source(
        self, native(chdl), [driver](unit_t&) { driver->init(); },
        [driver](unit_t&, caf::downstream<data_message>& out, size_t hint) {
          driver->pull(out.buf(), hint);
        },
        [driver](const unit_t&) { return driver->at_end(); });
      std::unique_lock<std::mutex> guard{mx};
      cv.notify_one();
    };
    auto res = make_worker(std::move(act));
    std::unique_lock<std::mutex> guard{mx};
    cv.wait(guard);
    return res;
  } else {
    auto act = [driver, chdl{core()}](self_ptr self) {
      caf::attach_stream_source(
        self, native(chdl), [driver](unit_t&) { driver->init(); },
        [driver](unit_t&, caf::downstream<data_message>& out, size_t hint) {
          driver->pull(out.buf(), hint);
        },
        [driver](const unit_t&) { return driver->at_end(); });
    };
    return make_worker(std::move(act));
  }
}

expected<store> endpoint::attach_master(std::string name, backend type,
                                        backend_options opts) {
  BROKER_INFO("attaching master store" << name << "of type" << type);
  expected<store> res{ec::unspecified};
  caf::scoped_actor self{ctx_->sys};
  self
    ->request(native(core_), caf::infinite, atom::store_v, atom::master_v,
              atom::attach_v, name, type, std::move(opts))
    .receive(
      [&](caf::actor& master) {
        res = store{facade(master), std::move(name)};
      },
      [&](caf::error& e) { res = facade(e); });
  return res;
}

expected<store> endpoint::attach_clone(std::string name,
                                       double resync_interval,
                                       double stale_interval,
                                       double mutation_buffer_interval) {
  BROKER_INFO("attaching clone store" << name);
  expected<store> res{ec::unspecified};
  caf::scoped_actor self{ctx_->sys};
  self
    ->request(native(core_), caf::infinite, atom::store_v, atom::clone_v,
              atom::attach_v, name, resync_interval, stale_interval,
              mutation_buffer_interval)
    .receive(
      [&](caf::actor& clone) {
        res = store{facade(clone), std::move(name)};
      },
      [&](caf::error& e) { res = facade(e); });
  return res;
}

void endpoint::wait_for(worker who) {
  caf::scoped_actor tmp{ctx_->sys};
  tmp->wait_for(native(who));
}

} // namespace broker

namespace broker::internal {

caf::actor_system& endpoint_access::sys() {
  return ep->ctx_->sys;
}

const caf::actor_system_config& endpoint_access::cfg() {
  return nat_cfg(ep->ctx_->cfg);
}

} // namespace broker::internal
