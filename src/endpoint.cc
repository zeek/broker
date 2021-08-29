#include <iostream>
#include <unordered_set>
#include <thread>

#include <caf/actor.hpp>
#include <caf/actor_system.hpp>
#include <caf/config.hpp>
#include <caf/error.hpp>
#include <caf/exit_reason.hpp>
#include <caf/io/network/default_multiplexer.hpp>
#include <caf/message.hpp>
#include <caf/node_id.hpp>
#include <caf/scheduled_actor/flow.hpp>
#include <caf/scheduler/test_coordinator.hpp>
#include <caf/scoped_actor.hpp>
#include <caf/send.hpp>

#include "broker/core_actor.hh"
#include "broker/defaults.hh"
#include "broker/detail/connector.hh"
#include "broker/detail/die.hh"
#include "broker/detail/filesystem.hh"
#include "broker/detail/flow_controller_callback.hh"
#include "broker/detail/telemetry/exporter.hh"
#include "broker/detail/telemetry/prometheus.hh"
#include "broker/endpoint.hh"
#include "broker/fwd.hh"
#include "broker/logger.hh"
#include "broker/publisher.hh"
#include "broker/status_subscriber.hh"
#include "broker/subscriber.hh"
#include "broker/timeout.hh"

namespace broker {

// --- async helper ------------------------------------------------------------

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
using async_helper_actor
  = caf::stateful_actor<async_helper_state<OnValue, OnError>>;

} // namespace

// --- nested classes ----------------------------------------------------------

endpoint::clock::clock(caf::actor_system* sys, bool use_real_time)
  : sys_(sys),
    real_time_(use_real_time),
    time_since_epoch_(),
    mtx_(),
    pending_(),
    pending_count_() {
  // nop
}

timestamp endpoint::clock::now() const noexcept {
  return real_time_ ? broker::now() : timestamp{time_since_epoch_};
}

void endpoint::clock::advance_time(timestamp t) {
  if (real_time_)
    return;

  if (t <= timestamp{time_since_epoch_})
    return;

  time_since_epoch_ = t.time_since_epoch();

  if (pending_count_ == 0)
    return;

  lock_type guard{mtx_};

  auto it = pending_.begin();

  if (it->first > t)
    return;

  // Note: this function is performance-sensitive in the case of Zeek
  // reading pcaps and it's important to not construct this set unless
  // it's actually going to be used.
  std::unordered_set<caf::actor> sync_with_actors;

  while (it != pending_.end() && it->first <= t) {
    auto& pm = it->second;
    caf::anon_send(pm.first, std::move(pm.second));
    sync_with_actors.emplace(pm.first);
    it = pending_.erase(it);
    --pending_count_;
  }

  guard.unlock();

  caf::scoped_actor self{*sys_};
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
      }
    );
  }
}

void endpoint::clock::send_later(caf::actor dest, timespan after,
                                 caf::message msg) {
  if (real_time_) {
    auto& sc = sys_->clock();
    auto t = sc.now() + after;
    auto me = caf::make_mailbox_element(nullptr, caf::make_message_id(),
                                        caf::no_stages, std::move(msg));
    sc.schedule_message(t, caf::actor_cast<caf::strong_actor_ptr>(dest),
                        std::move(me));
    return;
  }
  lock_type guard{mtx_};
  auto t = this->now() + after;
  pending_.emplace(t, pending_msg_type{std::move(dest), std::move(msg)});
  ++pending_count_;
}

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
      return maybe_dptr.error();
    }
    auto actual_port = dptr->port();
    using impl = detail::telemetry::prometheus_actor;
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
    caf::anon_send(parent_->telemetry_exporter_, atom::put_v, new_interval);
}

void endpoint::metrics_exporter_t::set_target(topic new_target) {
  if (!new_target.empty())
    caf::anon_send(parent_->telemetry_exporter_, atom::put_v,
                   std::move(new_target));
}

void endpoint::metrics_exporter_t::set_id(std::string new_id) {
  if (!new_id.empty())
    caf::anon_send(parent_->telemetry_exporter_, atom::put_v,
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
  caf::anon_send(parent_->telemetry_exporter_, atom::put_v, std::move(boxed));
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

  ~connector_task() {
    if (connector_) {
      connector_->async_shutdown();
      thread_.join();
    }
  }

  detail::connector_ptr start(caf::actor_system& sys, endpoint_id this_peer) {
    connector_ = std::make_shared<detail::connector>(this_peer);
    thread_ = std::thread{[ptr{connector_}, sys_ptr{&sys}] {
      CAF_SET_LOGGER_SYS(sys_ptr);
      ptr->run();
    }};
    return connector_;
  }

private:
  std::shared_ptr<detail::connector> connector_;
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

endpoint::endpoint(configuration config)
  : endpoint(std::move(config), endpoint_id::random()) {
  // nop
}

endpoint::endpoint(configuration config, endpoint_id this_peer)
  : config_(std::move(config)), id_(this_peer), destroyed_(false) {
  // Stop immediately if any helptext was printed.
  if (config_.cli_helptext_printed)
    exit(0);
  // Create a directory for storing the meta data if requested.
  auto meta_dir = get_or(config_, "broker.recording-directory",
                         defaults::recording_directory);
  if (!meta_dir.empty()) {
    if (detail::is_directory(meta_dir))
      detail::remove_all(meta_dir);
    if (detail::mkdirs(meta_dir)) {
      auto dump = config_.dump_content();
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
  // Spin up the actor system.
  new (&system_) caf::actor_system(config_);
  // Spin up the connector unless disabled via config.
  detail::connector_ptr conn_ptr;
  if (!caf::get_or(config_, "broker.disable-connector", false)) {
    auto conn_task = std::make_unique<connector_task>();
    conn_ptr = conn_task->start(system_, id_);
    background_tasks_.emplace_back(std::move(conn_task));
  } else {
    BROKER_DEBUG("run without a connector (assuming test mode)");
  }
  // Initialize remaining state.
  auto opts = config_.options();
  clock_ = new clock(&system_, opts.use_real_time);
  BROKER_INFO("creating endpoint");
  core_ = system_.spawn<core_actor_type>(id_, filter_type{}, clock_, nullptr,
                                         std::move(conn_ptr));
  // Spin up a Prometheus actor if configured or an exporter.
  namespace dt = detail::telemetry;
  if (auto port = caf::get_as<uint16_t>(config_, "broker.metrics.port")) {
    auto ptask = std::make_unique<prometheus_http_task>(system_);
    auto addr = caf::get_or(config_, "broker.metrics.address", std::string{});
    if (auto actual_port = ptask->start(*port, core_,
                                        addr.empty() ? nullptr : addr.c_str(),
                                        false)) {
      BROKER_INFO("expose metrics on port" << *actual_port);
      telemetry_exporter_ = ptask->telemetry_exporter();
      background_tasks_.emplace_back(std::move(ptask));
    } else {
      BROKER_ERROR("failed to expose metrics:" << actual_port.error());
    }
  } else {
    auto params = dt::exporter_params::from(config_);
    telemetry_exporter_
      = system_.spawn<dt::exporter_actor>(core_, std::move(params));
  }
}

endpoint::~endpoint() {
  shutdown();
}

void endpoint::shutdown() {
  if (destroyed_)
    return;
  // Lifetime scope of the BROKER_TRACE object: must go out of scope before
  // calling the destructor of caf::actor_system.
  {
    BROKER_TRACE("");
    BROKER_INFO("shutting down endpoint");
    caf::scoped_actor self{system_};
    BROKER_DEBUG("send shutdown message to core actor");
    self->monitor(core_);
    self->send(core_, atom::shutdown_v, shutdown_options_);
    self->receive( // Give the core 5s time to shut down gracefully.
      [](const caf::down_msg&) {},
      caf::after(std::chrono::seconds(5)) >>
        [&] {
          BROKER_WARNING("endpoint failed to shut down gracefully, kill");
          self->send_exit(core_, caf::exit_reason::kill);
          self->wait_for(core_);
        });
    if (!activities_.empty()) {
      BROKER_DEBUG("cancel background activities");
      for (auto& hdl : activities_)
        hdl.cancel();
      BROKER_DEBUG("wait until all background activities have completed");
      for (auto& hdl : activities_)
        hdl.wait();
      activities_.clear();
    }
    BROKER_DEBUG("stop" << background_tasks_.size() << "background tasks");
    background_tasks_.clear();
    BROKER_DEBUG("destroy actor system (final shutdown step)");
  }
  BROKER_DEBUG("stop background tasks");
  telemetry_exporter_ = nullptr;
  background_tasks_.clear();
  BROKER_DEBUG("send shutdown message to core actor");
  anon_send(core_, atom::shutdown_v);
  destroyed_ = true;
  core_ = nullptr;
  // TODO: there's got to be a better solution than calling the test coordinator
  //       manually, or at least push this to the destructor of actor_system.
  using caf::scheduler::test_coordinator;
  if (auto sched = dynamic_cast<test_coordinator*>(&system_.scheduler()))
    sched->run();
  system_.~actor_system();
  delete clock_;
  clock_ = nullptr;
}

uint16_t endpoint::listen(const std::string& address, uint16_t port) {
  BROKER_TRACE(BROKER_ARG(address) << BROKER_ARG(port));
  BROKER_INFO("try listening on"
              << (address + ":" + std::to_string(port))
              << (config_.options().disable_ssl ? "(no SSL)" : "(SSL)"));
  char const* addr = address.empty() ? nullptr : address.c_str();
  uint16_t result = 0;
  caf::scoped_actor self{system_};
  self->request(core_, caf::infinite, atom::listen_v, address, port)
    .receive(
      [&](atom::listen, atom::ok, uint16_t res) {
        BROKER_DEBUG("listening on port" << res);
        BROKER_ASSERT(res != 0);
        result = res;
      },
      [&](caf::error& err) {
        BROKER_DEBUG("cannot listen to" << address << "on port" << port << ":"
                                        << err);
      });
  return result;
}

bool endpoint::peer(const std::string& address, uint16_t port,
                    timeout::seconds retry) {
  BROKER_TRACE(BROKER_ARG(address) << BROKER_ARG(port) << BROKER_ARG(retry));
  BROKER_INFO("starting to peer with" << (address + ":" + std::to_string(port))
                                      << "retry:" << to_string(retry)
                                      << "[synchronous]");
  bool result = false;
  caf::scoped_actor self{system_};
  self
    ->request(core_, caf::infinite, atom::peer_v,
              network_info{address, port, retry})
    .receive([&](atom::peer, atom::ok, endpoint_id) { result = true; },
             [&](caf::error& err) {
               BROKER_DEBUG("cannot peer to" << address << "on port" << port
                                             << ":" << err);
             });
  return result;
}

bool endpoint::peer(const caf::uri& locator, timeout::seconds retry) {
  BROKER_TRACE(BROKER_ARG(locator) << BROKER_ARG(retry));
  if (auto info = to<network_info>(locator)) {
    return peer(info->address, info->port, retry);
  } else {
    BROKER_INFO("invalid URI:" << locator);
    return false;
  }
}

bool endpoint::mock_peer(detail::native_socket fd, endpoint_id peer_id,
                         std::string fake_host, filter_type filter) {
  BROKER_TRACE(BROKER_ARG(fd) << BROKER_ARG(peer_id) << BROKER_ARG(fake_host)
                              << BROKER_ARG(filter));
  bool result = false;
  caf::scoped_actor self{system_};
  self
    ->request(core_, caf::infinite, atom::peer_v, fd, peer_id,
              std::move(fake_host), std::move(filter))
    .receive([&] { result = true; },
             [&](caf::error& err) {
               BROKER_ERROR("mock_peer failed:" << err);
             });
  return result;
}

void endpoint::peer_nosync(const std::string& address, uint16_t port,
			   timeout::seconds retry) {
  BROKER_TRACE(BROKER_ARG(address) << BROKER_ARG(port));
  BROKER_INFO("starting to peer with" << (address + ":" + std::to_string(port))
                                      << "retry:" << to_string(retry)
                                      << "[asynchronous]");
  caf::anon_send(core(), atom::peer_v, network_info{address, port, retry});
}

std::future<bool> endpoint::peer_async(std::string host, uint16_t port,
                                       timeout::seconds retry) {
  BROKER_TRACE(BROKER_ARG(host) << BROKER_ARG(port));
  auto prom = std::make_shared<std::promise<bool>>();
  auto res = prom->get_future();
  auto on_val = [prom](atom::peer, atom::ok, endpoint_id) mutable {
    prom->set_value(true);
  };
  auto on_err = [prom](const caf::error&) {
    prom->set_value(false);
  };
  using actor_t = async_helper_actor<decltype(on_val), decltype(on_err)>;
  auto msg = caf::make_message(atom::peer_v,
                               network_info{std::move(host), port, retry});
  system_.spawn<actor_t>(core_, std::move(msg), std::move(on_val),
                         std::move(on_err));
  return res;
}

bool endpoint::unpeer(const std::string& address, uint16_t port) {
  BROKER_TRACE(BROKER_ARG(address) << BROKER_ARG(port));
  BROKER_INFO("stopping to peer with" << address << ":" << port
                                      << "[synchronous]");
  bool result = false;
  caf::scoped_actor self{system_};
  self->request(core_, caf::infinite, atom::unpeer_v,
                network_info{address, port})
  .receive(
    [&](void) {
      result = true;
    },
    [&](caf::error& err) {
      BROKER_DEBUG("Cannot unpeer from" << address << "on port"
                    << port << ":" << err);
    }
  );

  return result;
}

void endpoint::unpeer_nosync(const std::string& address, uint16_t port) {
  BROKER_TRACE(BROKER_ARG(address) << BROKER_ARG(port));
  BROKER_INFO("stopping to peer with " << address << ":" << port
                                       << "[asynchronous]");
  caf::anon_send(core(), atom::unpeer_v, network_info{address, port});
}

std::vector<peer_info> endpoint::peers() const {
  std::vector<peer_info> result;
  caf::scoped_actor self{system_};
  self->request(core(), caf::infinite, atom::get_v, atom::peer_v)
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
  caf::scoped_actor self{system_};
  self->request(core(), caf::infinite, atom::get_v,
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
  caf::anon_send(core(), atom::subscribe_v, std::move(ts));
}

void endpoint::publish(topic t, data d) {
  BROKER_INFO("publishing" << std::make_pair(t, d));
  caf::anon_send(core(), atom::publish_v,
                 make_data_message(std::move(t), std::move(d)));
}

void endpoint::publish(const endpoint_info& dst, topic t, data d) {
  BROKER_INFO("publishing" << std::make_pair(t, d) << "to" << dst.node);
  caf::anon_send(core(), atom::publish_v, dst,
                 make_data_message(std::move(t), std::move(d)));
}

void endpoint::publish(data_message x){
  BROKER_INFO("publishing" << x);
  caf::anon_send(core(), atom::publish_v, std::move(x));
}


void endpoint::publish(std::vector<data_message> xs) {
  BROKER_INFO("publishing" << xs.size() << "messages");
  for (auto& x : xs)
    publish(std::move(x));
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

activity endpoint::do_subscribe(filter_type filter,
                                detail::sink_driver_ptr sink) {
  BROKER_ASSERT(sink != nullptr);
  using observer_t = caf::flow::observer<data_message>;
  auto hdl_prm = std::make_shared<std::promise<caf::actor>>();
  auto hdl_fut = hdl_prm->get_future();
  auto cb = detail::make_flow_controller_callback(
    [flt{std::move(filter)}, snk{std::move(sink)}, prm{std::move(hdl_prm)}] //
    (detail::flow_controller * ctrl) mutable {
      ctrl->add_filter(flt);
      ctrl->select_local_data(flt).subscribe_with<worker_actor>(
        ctrl->ctx()->system(), [&](auto* self, auto in) {
          snk->init();
          in.attach(observer_t{std::move(snk)});
          prm->set_value(caf::actor{self});
        });
    });
  caf::anon_send(core(), std::move(cb));
  auto hdl = hdl_fut.get();
  activities_.emplace_back(activity{std::move(hdl)});
  return activities_.back();
}

activity endpoint::do_subscribe_nosync(filter_type filter,
                                       detail::sink_driver_ptr sink) {
  using observer_t = caf::flow::observer<data_message>;
  auto hdl = system_.spawn([=](worker_actor* self) -> caf::behavior {
    return {
      [self, sink](detail::data_message_publisher pub) {
        self->observe(pub.hdl).attach(observer_t{sink});
      },
    };
  });
  auto cb = detail::make_flow_controller_callback(
    [flt{std::move(filter)}, hdl](detail::flow_controller* ctrl) mutable {
      ctrl->add_filter(flt);
      auto pub = ctrl->select_local_data(flt);
      caf::anon_send(hdl, detail::data_message_publisher{std::move(pub)});
    });
  caf::anon_send(core(), std::move(cb));
  activities_.emplace_back(activity{std::move(hdl)});
  return activities_.back();
}

expected<store> endpoint::attach_master(std::string name, backend type,
                                        backend_options opts) {
  BROKER_TRACE(BROKER_ARG(name) << BROKER_ARG(type) << BROKER_ARG(opts));
  BROKER_INFO("attaching master store" << name << "of type" << type);
  expected<store> res{ec::unspecified};
  caf::scoped_actor self{system_};
  self
    ->request(core(), caf::infinite, atom::store_v, atom::master_v,
              atom::attach_v, name, type, std::move(opts))
    .receive(
      [&](caf::actor& master) {
        res = store{id_, std::move(master), std::move(name)};
      },
      [&](caf::error& e) { res = std::move(e); });
  return res;
}

expected<store> endpoint::attach_clone(std::string name,
                                       double resync_interval,
                                       double stale_interval,
                                       double mutation_buffer_interval) {
  BROKER_TRACE(BROKER_ARG(name)
               << BROKER_ARG(resync_interval) << BROKER_ARG(stale_interval)
               << BROKER_ARG(mutation_buffer_interval));
  BROKER_INFO("attaching clone store" << name);
  expected<store> res{ec::unspecified};
  caf::scoped_actor self{core()->home_system()};
  self
    ->request(core(), caf::infinite, atom::store_v, atom::clone_v,
              atom::attach_v, name, resync_interval, stale_interval,
              mutation_buffer_interval)
    .receive(
      [&](caf::actor& clone) {
        res = store{id_, std::move(clone), std::move(name)};
      },
      [&](caf::error& e) { res = std::move(e); });
  return res;
}

std::future<expected<store>>
endpoint::attach_clone_async(std::string name, double resync_interval,
                       double stale_interval, double mutation_buffer_interval) {
  BROKER_TRACE(BROKER_ARG(name)
               << BROKER_ARG(resync_interval) << BROKER_ARG(stale_interval)
               << BROKER_ARG(mutation_buffer_interval));
  BROKER_INFO("attaching clone store" << name);
  using res_t = expected<store>;
  auto prom = std::make_shared<std::promise<res_t>>();
  auto res = prom->get_future();
  auto on_val = [id{id_}, prom, name](caf::actor& clone) mutable {
    prom->set_value(res_t{store{id, std::move(clone), std::move(name)}});
  };
  auto on_err = [prom](caf::error& err) {
    prom->set_value(res_t{std::move(err)});
  };
  using actor_t = async_helper_actor<decltype(on_val), decltype(on_err)>;
  auto msg = caf::make_message(atom::store_v, atom::clone_v, atom::attach_v,
                               name, resync_interval, stale_interval,
                               mutation_buffer_interval);
  system_.spawn<actor_t>(core_, std::move(msg), std::move(on_val),
                         std::move(on_err));
  return res;
}

bool endpoint::await_peer(endpoint_id whom, timespan timeout) {
  BROKER_TRACE(BROKER_ARG(whom) << BROKER_ARG(timeout));
  bool result = false;
  caf::scoped_actor self{core()->home_system()};
  self->request(core(), timeout, atom::await_v, whom)
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
  BROKER_TRACE(BROKER_ARG(whom) << BROKER_ARG(timeout));
  if (!callback) {
    BROKER_ERROR("invalid callback received for await_peer");
    return;
  }
  auto f = [whom, cb{std::move(callback)}](caf::event_based_actor* self,
                                           caf::actor core, timespan t) {
    self->request(core, t, atom::await_v, whom)
      .then(
        [&]([[maybe_unused]] endpoint_id& discovered) {
          BROKER_ASSERT(whom == discovered);
          cb(true);
        },
        [&](caf::error& e) { cb(false); });
  };
  core()->home_system().spawn(f, core(), timeout);
}

} // namespace broker
