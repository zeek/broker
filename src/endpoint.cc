#include <iostream>
#include <unordered_set>

#include <caf/actor.hpp>
#include <caf/actor_system.hpp>
#include <caf/config.hpp>
#include <caf/error.hpp>
#include <caf/exit_reason.hpp>
#include <caf/message.hpp>
#include <caf/node_id.hpp>
#include <caf/scheduled_actor/flow.hpp>
#include <caf/scoped_actor.hpp>
#include <caf/send.hpp>

#include "broker/core_actor.hh"
#include "broker/defaults.hh"
#include "broker/detail/connector.hh"
#include "broker/detail/die.hh"
#include "broker/detail/filesystem.hh"
#include "broker/detail/flow_controller_callback.hh"
#include "broker/endpoint.hh"
#include "broker/fwd.hh"
#include "broker/logger.hh"
#include "broker/publisher.hh"
#include "broker/status_subscriber.hh"
#include "broker/subscriber.hh"
#include "broker/timeout.hh"

namespace broker {

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

endpoint::endpoint(configuration config)
  : config_(std::move(config)), id_(endpoint_id::random()), destroyed_(false) {
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
  // Spin up the connector.
  auto conn_task = std::make_unique<connector_task>();
  auto conn_ptr = conn_task->start(system_, id_);
  background_tasks_.emplace_back(std::move(conn_task));
  // Initialize remaining state.
  auto opts = config_.options();
  clock_ = new clock(&system_, opts.use_real_time);
  BROKER_INFO("creating endpoint");
  core_ = system_.spawn<core_actor_type>(id_, filter_type{}, clock_, nullptr,
                                         std::move(conn_ptr));
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
  destroyed_ = true;
  core_ = nullptr;
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

void endpoint::peer_nosync(const std::string& address, uint16_t port,
			   timeout::seconds retry) {
  BROKER_TRACE(BROKER_ARG(address) << BROKER_ARG(port));
  BROKER_INFO("starting to peer with" << (address + ":" + std::to_string(port))
                                      << "retry:" << to_string(retry)
                                      << "[asynchronous]");
  caf::anon_send(core(), atom::peer_v, network_info{address, port, retry});
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
