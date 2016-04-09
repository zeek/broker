#include "broker/store/clone.hh"

namespace broker {

extern std::unique_ptr<caf::actor_system> broker_system;

namespace store {
namespace detail {

clone_actor::clone_actor(caf::actor_config& cfg, const caf::actor& endpoint,
                         identifier master_name,
                         std::chrono::microseconds resync_interval,
                         std::unique_ptr<backend> b)
  : caf::event_based_actor{cfg},
    datastore_(std::move(b)) {
  using namespace std;
  using namespace caf;
  message_handler requests{
    [=](const identifier& n, const query& q, const actor& requester) {
      auto r = q.process(*datastore_, broker::time_point::now().value).first;
      if (r.stat == result::status::failure)
        error(master_name, "process query", datastore_->last_error());
      return make_message(this, move(r));
    }
  };
  message_handler updates{
    on(val<identifier>, any_vals) >> [=] {
      forward_to(master);
    },
    [=](increment_atom, const sequence_num& sn, const data& k, int64_t by,
        double mod_time) {
      auto next = datastore_->sequence().next();
      if (sn == next) {
        if (datastore_->increment(k, by, mod_time).stat
            != modification_result::status::success)
          error(master_name, "increment", datastore_->last_error(), true);
      } else if (sn > next)
        sequence_error(master_name, resync_interval);
    },
    [=](set_add_atom, const sequence_num& sn, const data& k, data& e,
        double mod_time) {
      auto next = datastore_->sequence().next();
      if (sn == next) {
        if (datastore_->add_to_set(k, std::move(e), mod_time).stat
            != modification_result::status::success)
          error(master_name, "add_to_set", datastore_->last_error(), true);
      } else if (sn > next)
        sequence_error(master_name, resync_interval);
    },
    [=](set_rem_atom, const sequence_num& sn, const data& k, const data& e,
        double mod_time) {
      auto next = datastore_->sequence().next();
      if (sn == next) {
        if (datastore_->remove_from_set(k, e, mod_time).stat
            != modification_result::status::success)
          error(master_name, "remove_from_set", datastore_->last_error(),
                true);
      } else if (sn > next)
        sequence_error(master_name, resync_interval);
    },
    [=](insert_atom, const sequence_num& sn, data& k, data& v) {
      auto next = datastore_->sequence().next();
      if (sn == next) {
        if (!datastore_->insert(move(k), move(v)))
          fatal_error(master_name, "insert", datastore_->last_error());
      } else if (sn > next)
        sequence_error(master_name, resync_interval);
    },
    [=](insert_atom, const sequence_num& sn, data& k, data& v,
        expiration_time t) {
      auto next = datastore_->sequence().next();
      if (sn == next) {
        if (!datastore_->insert(move(k), move(v), t))
          fatal_error(master_name, "insert_with_expiry",
                      datastore_->last_error());
      } else if (sn > next)
        sequence_error(master_name, resync_interval);
    },
    [=](erase_atom, const sequence_num& sn, const data& k) {
      auto next = datastore_->sequence().next();
      if (sn == next) {
        if (!datastore_->erase(k))
          fatal_error(master_name, "erase", datastore_->last_error());
      } else if (sn > next)
        sequence_error(master_name, resync_interval);
    },
    [=](expire_atom, const sequence_num& sn, const data& k,
        const expiration_time& expiry) {
      auto next = datastore_->sequence().next();
      if (sn == next) {
        if (!datastore_->expire(k, expiry))
          fatal_error(master_name, "expire", datastore_->last_error());
      } else if (sn > next)
        sequence_error(master_name, resync_interval);
    },
    [=](clear_atom, const sequence_num& sn) {
      auto next = datastore_->sequence().next();
      if (sn == next) {
        if (!datastore_->clear())
          fatal_error(master_name, "clear", datastore_->last_error());
      } else if (sn > next)
        sequence_error(master_name, resync_interval);
    },
    [=](lpush_atom, const sequence_num& sn, const data& k, broker::vector& i,
        double mod_time) {
      auto next = datastore_->sequence().next();
      if (sn == next) {
        if (datastore_->push_left(k, std::move(i), mod_time).stat
            != modification_result::status::success)
          error(master_name, "push_left", datastore_->last_error(), true);
      } else if (sn > next)
        sequence_error(master_name, resync_interval);
    },
    [=](rpush_atom, const sequence_num& sn, const data& k, broker::vector& i,
        double mod_time) {
      auto next = datastore_->sequence().next();
      if (sn == next) {
        if (datastore_->push_right(k, std::move(i), mod_time).stat
            != modification_result::status::success)
          error(master_name, "push_right", datastore_->last_error(), true);
      } else if (sn > next)
        sequence_error(master_name, resync_interval);
    },
    [=](lpop_atom, const sequence_num& sn, const data& k, double mod_time) {
      auto next = datastore_->sequence().next();
      if (sn == next) {
        if (datastore_->pop_left(k, mod_time).first.stat
            != modification_result::status::success)
          error(master_name, "pop_left", datastore_->last_error(), true);
      } else if (sn > next)
        sequence_error(master_name, resync_interval);
    },
    [=](rpop_atom, const sequence_num& sn, const data& k, double mod_time) {
      auto next = datastore_->sequence().next();
      if (sn == next) {
        if (datastore_->pop_right(k, mod_time).first.stat
            != modification_result::status::success)
          error(master_name, "pop_right", datastore_->last_error(), true);
      } else if (sn > next)
        sequence_error(master_name, resync_interval);
    }
  };
  bootstrap_ = {
    after(chrono::seconds::zero()) >> [=] {
      send(this, find_master_atom::value);
      get_snapshot(chrono::seconds::zero());
      become(synchronizing_);
    }
  };
  message_handler give_actor{
    [=](store_actor_atom, const identifier& n) -> actor {
      return this;
    }
  };
  message_handler find_master{
    [=](find_master_atom) {
      request(endpoint, infinite, store_actor_atom::value, master_name).then(
        [=](actor& m) {
          if (m) {
            BROKER_DEBUG("store.clone." + master_name, "Located master");
            demonitor(master);
            master = move(m);
            monitor(master);
          } else {
            BROKER_DEBUG("store.clone." + master_name,
                         "Failed to locate master, will retry...");
            delayed_send(this, resync_interval, find_master_atom::value);
          }
        });
    },
    [=](const down_msg& d) {
      if (d.source == master.address()) {
        demonitor(master);
        master = caf::invalid_actor;
        BROKER_DEBUG("store.clone." + master_name,
                     "master went down, trying to relocate...");
        send(this, find_master_atom::value);
        get_snapshot(resync_interval);
      }
    }
  };
  message_handler get_snap{
    [=](get_snap_atom) {
      pending_getsnap_ = false;
      if (!master) {
        get_snapshot(resync_interval);
        return;
      }
      request(master, infinite, master_name, query(query::tag::snapshot),
              this).then(
        [=](actor& responder, result& r) {
          if (r.stat != result::status::success
              || r.value.which() != result::tag::snapshot_result) {
            BROKER_DEBUG("store.clone." + master_name,
                         "got invalid snapshot response, retry...");
            get_snapshot(resync_interval);
          } else {
            if (datastore_->init(move(*get<snapshot>(r.value)))) {
              BROKER_DEBUG("store.clone." + master_name,
                           "successful init from snapshot");
              become(active_);
            } else
              fatal_error(master_name, "init", datastore_->last_error());
          }
        },
        [=](const caf::error& err) {
          if (err == sec::request_receiver_down) {
            BROKER_DEBUG("store.clone." + master_name,
                         "master went down while requesting snapshot,"
                         " will retry...");
            get_snapshot(resync_interval);
          } else {
            assert(!"unhandled request error");
          }
        });
    }
  };
  auto handlesync = find_master.or_else(get_snap).or_else(give_actor);
  synchronizing_ = handlesync;
  active_ = requests.or_else(updates).or_else(handlesync);
  dead_ = requests.or_else(give_actor).or_else(others() >> [] {});
}

caf::behavior clone_actor::make_behavior() {
  return bootstrap_;
}

void clone_actor::error(std::string master_name, std::string method_name,
           std::string err_msg, bool fatal) {
  report::error("store.clone." + master_name,
                "failed to " + method_name + ": " + err_msg);
  if (fatal)
    become(dead_);
}

void clone_actor::fatal_error(std::string master_name, std::string method_name,
                 std::string err_msg) {
  error(master_name, method_name, err_msg, true);
}

void
clone_actor::get_snapshot(const std::chrono::microseconds& resync_interval) {
  if (pending_getsnap_)
    return;
  delayed_send(this, resync_interval, get_snap_atom::value);
  pending_getsnap_ = true;
}

void
clone_actor::sequence_error(const identifier& master_name,
                            const std::chrono::microseconds& resync_interval) {
  report::error("store.clone." + master_name, "got desynchronized");
  get_snapshot(resync_interval);
}

} // namespace detail

clone::clone(const endpoint& e, identifier master_name,
             std::chrono::duration<double> ri, std::unique_ptr<backend> b)
  : frontend{e, master_name},
    self_{*broker_system} {
    auto resync_interval =
      std::chrono::duration_cast<std::chrono::microseconds>(ri);
    // FIXME: rocksdb backend should also be detached, but why does
    // rocksdb::~DB then crash?
    if (dynamic_cast<sqlite_backend*>(b.get()))
      actor_ = broker_system->spawn<detail::clone_actor, caf::detached>(
        *static_cast<caf::actor*>(e.handle()), std::move(master_name),
        resync_interval, std::move(b));
    else
      actor_ = broker_system->spawn<detail::clone_actor>(
        *static_cast<caf::actor*>(e.handle()), std::move(master_name),
        resync_interval, std::move(b));
    // FIXME: do not rely on private API.
    self_->planned_exit_reason(caf::exit_reason::unknown);
    actor_->link_to(self_);
}

void* clone::handle() const {
  return const_cast<caf::actor*>(&actor_);
}

} // namespace store
} // namespace broker

// Begin C API
#include "broker/broker.h"

broker_store_frontend* broker_store_clone_create_memory(
  const broker_endpoint* e, const broker_string* name, double resync_interval) {
  auto ee = reinterpret_cast<const broker::endpoint*>(e);
  auto nn = reinterpret_cast<const std::string*>(name);
  auto rr = std::chrono::duration<double>(resync_interval);
  try {
    auto rval = new broker::store::clone(*ee, *nn, rr);
    return reinterpret_cast<broker_store_frontend*>(rval);
  } catch (std::bad_alloc&) {
    return nullptr;
  }
}

broker_store_frontend* broker_store_clone_create_sqlite(
  const broker_endpoint* e, const broker_string* name, double resync_interval,
  broker_store_sqlite_backend* b) {
  auto ee = reinterpret_cast<const broker::endpoint*>(e);
  auto nn = reinterpret_cast<const std::string*>(name);
  auto bb = reinterpret_cast<broker::store::sqlite_backend*>(b);
  auto rr = std::chrono::duration<double>(resync_interval);
  try {
    std::unique_ptr<broker::store::backend> bp(bb);
    auto rval = new broker::store::clone(*ee, *nn, rr, std::move(bp));
    return reinterpret_cast<broker_store_frontend*>(rval);
  } catch (std::bad_alloc&) {
    return nullptr;
  }
}

#ifdef HAVE_ROCKSDB
#include "broker/store/rocksdb_backend.hh"

broker_store_frontend* broker_store_clone_create_rocksdb(
  const broker_endpoint* e, const broker_string* name, double resync_interval,
  broker_store_rocksdb_backend* b) {
  auto ee = reinterpret_cast<const broker::endpoint*>(e);
  auto nn = reinterpret_cast<const std::string*>(name);
  auto bb = reinterpret_cast<broker::store::rocksdb_backend*>(b);
  auto rr = std::chrono::duration<double>(resync_interval);
  try {
    std::unique_ptr<broker::store::backend> bp(bb);
    auto rval = new broker::store::clone(*ee, *nn, rr, std::move(bp));
    return reinterpret_cast<broker_store_frontend*>(rval);
  } catch (std::bad_alloc&) {
    return nullptr;
  }
}

#endif // HAVE_ROCKSDB
