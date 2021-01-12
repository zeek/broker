#include "broker/detail/unipath_manager.hh"

#include <type_traits>

#include <caf/actor_system_config.hpp>
#include <caf/downstream_manager.hpp>
#include <caf/outbound_path.hpp>
#include <caf/scheduled_actor.hpp>
#include <caf/settings.hpp>
#include <caf/typed_message_view.hpp>

#include "broker/defaults.hh"
#include "broker/detail/assert.hh"
#include "broker/detail/central_dispatcher.hh"
#include "broker/detail/item.hh"
#include "broker/detail/prefix_matcher.hh"
#include "broker/logger.hh"
#include "broker/message.hh"
#include "broker/topic.hh"

namespace broker::detail {

namespace {

bool ends_with(caf::string_view str, caf::string_view suffix) {
  return str.size() >= suffix.size()
         && str.compare(str.size() - suffix.size(), suffix.size(), suffix) == 0;
}

void try_grant_more_credit(caf::stream_manager* mgr) {
  for (auto path : mgr->inbound_paths()) {
    if (path->hdl) {
      auto available = path->available_credit();
      if (available >= path->desired_batch_size
          || (path->assigned_credit == 0 && available > 0)) {
        if (auto acquired = mgr->acquire_credit(path, available); acquired > 0)
          path->emit_ack_batch(mgr->self(), acquired);
      }
    }
  }
}

// A downstream manager with at most one outbound path.
template <class T>
class unipath_downstream : public caf::downstream_manager {
public:
  using super = downstream_manager;

  using output_type = T;

  using buffer_type = std::deque<output_type>;

  using unique_path_ptr = std::unique_ptr<caf::outbound_path>;

  using super::super;

  ptrdiff_t enqueue(caf::span<const item_ptr> ptrs) {
    if (path_) {
      prefix_matcher accept;
      ptrdiff_t accepted = 0;
      for (auto&& ptr : ptrs) {
        const auto& msg = ptr->msg();
        if (ptr->origin() != super::parent()) {
          if constexpr (std::is_same<T, node_message>::value) {
            // Somewhat hacky, but don't forward data store clone messages.
            if (ptr->scope() != item_scope::local
                && ptr->ttl() > 0
                && accept(filter_, msg)) {
              cache_.emplace_back(make_node_message(msg, ptr->ttl() - 1));
              items_.emplace_back(ptr);
              ++accepted;
            }
          } else if (ptr->scope() != item_scope::remote
                     && caf::holds_alternative<T>(msg)) {
            const auto& unboxed = caf::get<T>(msg);
            if (accept(filter_, unboxed)) {
              cache_.emplace_back(unboxed);
              items_.emplace_back(ptr);
              ++accepted;
            }
          }
        }
      }
      BROKER_DEBUG(BROKER_ARG2("ptrs.size", ptrs.size())
                   << BROKER_ARG(accepted));
      return accepted;
    } else {
      BROKER_DEBUG("no path available"
                   << BROKER_ARG2("ptrs.size", ptrs.size()));
      return -1;
    }
  }

  bool has_path() const noexcept {
    return path_ != nullptr;
  }

  const caf::outbound_path* get_path() const noexcept {
    return path_.get();
  }

  size_t num_paths() const noexcept override {
    return path_ ? 1 : 0;
  }

  bool remove_path(caf::stream_slot x, caf::error reason,
                   bool silent) noexcept override {
    if (is_this_slot(x)) {
      super::about_to_erase(path_.get(), silent, &reason);
      path_.reset();
      cache_.clear();
      items_.clear();
      return true;
    } else {
      return false;
    }
  }

  super::path_ptr path(caf::stream_slot x) noexcept override {
    return is_this_slot(x) ? path_.get() : nullptr;
  }

  void emit_batches_impl(bool forced) {
    if (!cache_.empty()) {
      BROKER_DEBUG(BROKER_ARG2("cache.size", cache_.size()));
      BROKER_ASSERT(path_ != nullptr);
      auto old_size = cache_.size();
      path_->emit_batches(super::self(), cache_, forced || path_->closing);
      if (auto delta = old_size - cache_.size(); delta > 0) {
        // Get all stream managers that reclaim items as a result of us
        // releasing erasing elements from items_. Then drop the items and check
        // for each manager if they can grant new credit again.
        auto first = items_.begin();
        auto last = items_.begin() + delta;
        for (auto i = first; i != last; ++i)
          if ((*i)->unique() && (*i)->origin())
            mgr_cache_.emplace_back((*i)->origin());
        items_.erase(first, last);
        std::sort(mgr_cache_.begin(), mgr_cache_.end());
        auto e = std::unique(mgr_cache_.begin(), mgr_cache_.end());
        for (auto i = mgr_cache_.begin(); i != e; ++i)
          try_grant_more_credit(i->get());
        mgr_cache_.clear();
      }
    }
  }

  void emit_batches() override {
    emit_batches_impl(false);
  }

  void force_emit_batches() override {
    emit_batches_impl(true);
  }

  void clear_paths() override {
    path_.reset();
  }

  bool terminal() const noexcept override {
    return false;
  }

  size_t capacity() const noexcept override {
    // Our goal is to cache up to 2 full batches.
    if (path_) {
      auto want = static_cast<size_t>(path_->desired_batch_size) * 2;
      auto got = cache_.size();
      return want > got ? want - got : size_t{0};
    } else {
      return 0;
    }
  }

  size_t buffered() const noexcept override {
    return cache_.size();
  }

  size_t buffered(caf::stream_slot x) const noexcept override {
    return is_this_slot(x) ? cache_.size() : 0;
  }

  bool insert_path(unique_path_ptr ptr) override {
    using std::swap;
    if (!path_) {
      swap(path_, ptr);
      return true;
    } else {
      return false;
    }
  }

  void for_each_path_impl(super::path_visitor& f) override {
    if (path_)
      f(*path_);
  }

  bool check_paths_impl(super::path_algorithm algo,
                        super::path_predicate& pred) const noexcept override {
    if (path_) {
      switch (algo) {
        default:
          // all_of or any_of
          return pred(*path_);
        case super::path_algorithm::none_of:
          return !pred(*path_);
      }
    } else {
      return super::check_paths_impl(algo, pred);
    }
  }

  bool is_this_slot(caf::stream_slot x) const noexcept {
    return path_ && path_->slots.sender == x;
  }

  unique_path_ptr path_;
  filter_type filter_;
  std::vector<T> cache_;
  std::vector<item_ptr> items_;
  std::vector<caf::stream_manager_ptr> mgr_cache_;
};

template <class T>
class unipath_manager_out : public unipath_manager {
public:
  using super = unipath_manager;

  unipath_manager_out(central_dispatcher* dispatcher,
                      unipath_manager::observer* observer)
    : super(dispatcher, observer), out_(this) {
    // nop
  }

  template <class Filter>
  unipath_manager_out(central_dispatcher* dispatcher,
                      unipath_manager::observer* observer, Filter&& filter)
    : unipath_manager_out(dispatcher, observer) {
    out_.filter_ = std::forward<Filter>(filter);
  }

  ptrdiff_t enqueue(caf::span<const item_ptr> ptrs) override {
    return out_.enqueue(ptrs);
  }

  filter_type filter() override {
    return out_.filter_;
  }

  void filter(filter_type new_filter) override {
    out_.filter_ = std::move(new_filter);
  }

  bool accepts(const topic&t) const noexcept override {
    prefix_matcher accept;
    return accept(out_.filter_, t);
  }

  caf::type_id_t message_type() const noexcept override {
    return caf::type_id_v<T>;
  }

  caf::downstream_manager& out() override {
    return out_;
  }

  bool done() const override {
    auto open_paths = out_.num_paths()
                      + this->pending_handshakes_
                      + this->inbound_paths_.size();
    return open_paths == 0 || !super::self_->has_behavior();
  }

  bool idle() const noexcept override {
    // A source is idle if it can't make any progress on its downstream or if
    // it's not producing new data despite having credit.
    if (auto ptr = out_.get_path()) {
      return out_.stalled() || (out_.buffered() == 0 && ptr->open_credit > 0);
    } else {
      return true;
    }
  }

  using super::handle;

  bool handle(caf::stream_slots slots,
              caf::upstream_msg::ack_open& x) override {
    BROKER_TRACE(BROKER_ARG(slots) << BROKER_ARG(x));
    auto rebind_from = x.rebind_from;
    auto rebind_to = x.rebind_to;
    if (x.rebind_from != x.rebind_to) {
      BROKER_ERROR("unipath managers disallow rebinding!");
      this->closing(false, caf::sec::runtime_error);
      return false;
    } else if (caf::stream_manager::handle(slots, x)) {
      if (auto ptr = this->observer_)
        ptr->downstream_connected(this, caf::actor_cast<caf::actor>(rebind_to));
      return true;
    } else {
      BROKER_ERROR("unipath manager failed to process ack_open!");
      this->closing(false, caf::sec::runtime_error);
      return false;
    }
  }

protected:
  unipath_downstream<T> out_;
};

class unipath_manager_in_only : public unipath_manager {
public:
  using super = unipath_manager;

  unipath_manager_in_only(central_dispatcher* dispatcher,
                          unipath_manager::observer* observer)
    : super(dispatcher, observer), out_(this) {
    // nop
  }

  ptrdiff_t enqueue(caf::span<const item_ptr>) override {
    return -1;
  }

  filter_type filter() override {
    return {};
  }

  void filter(filter_type) override {
    // nop
  }

  bool accepts(const topic&) const noexcept override {
    return false;
  }

  caf::downstream_manager& out() override {
    return out_;
  }

  bool done() const override {
    return inbound_paths_.empty() || !super::self_->has_behavior();
  }

  bool idle() const noexcept override {
    return inbound_paths_idle();
  }

protected:
  caf::downstream_manager out_;
};

template <class T, class Base = unipath_manager_in_only>
class unipath_manager_in : public Base {
public:
  using super = Base;

  template <class... Ts>
  explicit unipath_manager_in(central_dispatcher* dispatcher,
                              unipath_manager::observer* observer, Ts&&... xs)
    : super(dispatcher, observer, std::forward<Ts>(xs)...) {
    auto sptr = super::self();
    auto& cfg = sptr->system().config();
    auto stash_size = caf::get_or(cfg, "broker.max-pending-inputs-per-source",
                                  defaults::max_pending_inputs_per_source);
    stash_ = dispatcher->new_item_stash(stash_size);
    if (caf::get_or(cfg, "broker.forward", true)) {
      ttl_ = caf::get_or(cfg, "broker.ttl", defaults::ttl);
    } else {
      // Limit TTL to 1 when forwarding was disabled. This causes all peer
      // managers to drop node messages instead of forwarding them.
      ttl_ = 1;
    }
  }

  void block_inputs() override {
    block_inputs_ = true;
  }

  void unblock_inputs() override {
    if (block_inputs_) {
      block_inputs_ = false;
      for (auto& batch : blocked_batches_)
        handle(nullptr, batch);
      blocked_batches_.clear();
    }
  }

  bool blocks_inputs() override {
    return block_inputs_;
  }

  caf::type_id_t message_type() const noexcept override {
    return caf::type_id_v<T>;
  }

  bool idle() const noexcept override {
    if constexpr (std::is_same<Base, unipath_manager_in_only>::value) {
      return super::idle();
    } else {
      auto& dm = this->out_;
      return dm.stalled() || (dm.clean() && this->inbound_paths_idle());
    }
  }

  using super::handle;

  void handle(caf::inbound_path*, caf::downstream_msg::batch& x) override {
    BROKER_TRACE(BROKER_ARG(x));
    BROKER_DEBUG(BROKER_ARG2("batch.size", x.xs_size)
                 << BROKER_ARG(block_inputs_));
    if (block_inputs_) {
      blocked_batches_.push_back(std::move(x));
    } else if (auto view = caf::make_typed_message_view<std::vector<T>>(x.xs)) {
      auto& vec = get<0>(view);
      if (vec.size() <= stash_->available()) {
        items_.reserve(vec.size());
        for (auto& x : vec) {
          item_ptr ptr;
          if constexpr (std::is_same<T, node_message>::value) {
            // Somewhat hacky, but don't forward data store clone messages.
            uint16_t ttl;
            if (ends_with(get_topic(x).string(), topics::clone_suffix.string()))
              ttl = 1;
            else
              ttl = std::min(ttl_, x.ttl);
            ptr = stash_->next_item(std::move(x.content), ttl, this);
          } else {
            ptr = stash_->next_item(std::move(x), ttl_, this,
                                    item_scope::remote);
          }
          items_.emplace_back(std::move(ptr));
        }
        super::dispatcher_->enqueue(items_);
        super::dispatcher_->ship();
        items_.clear();
        try_grant_more_credit(this);
      } else {
        BROKER_ERROR("received more data then we have allowed!");
      }
    } else {
      BROKER_ERROR("received unexpected batch type (dropped)");
    }
  }

  int32_t acquire_credit(caf::inbound_path* in, int32_t desired) override {
    auto available = static_cast<int32_t>(stash_->available());
    BROKER_ASSERT(in->assigned_credit <= available);
    auto total = in->assigned_credit + desired;
    if (total <= available)
      return desired;
    else
      return available - in->assigned_credit;
  }

private:
  item_stash_ptr stash_;
  std::vector<item_ptr> items_;
  uint16_t ttl_;
  bool block_inputs_ = false;
  std::vector<caf::downstream_msg::batch> blocked_batches_;
};

} // namespace

// -- unipath_manager ----------------------------------------------------------

unipath_manager::observer::~observer() {
  // nop
}

unipath_manager::unipath_manager(central_dispatcher* dispatcher, observer* obs)
  : super(dispatcher->self()), dispatcher_(dispatcher), observer_(obs) {
  // nop
}

unipath_manager::~unipath_manager() {
  // nop
}

void unipath_manager::block_inputs() {
  // nop
}

void unipath_manager::unblock_inputs() {
  // nop
}

bool unipath_manager::blocks_inputs() {
  return false;
}

bool unipath_manager::has_inbound_path() const noexcept {
  return inbound_paths().size() == 1;
}

bool unipath_manager::has_outbound_path() const noexcept {
  return out().num_paths() == 1;
}

caf::stream_slot unipath_manager::inbound_path_slot() const noexcept {
  if (auto& vec = inbound_paths(); vec.size() == 1)
    return vec[0]->slots.receiver;
  else
    return caf::invalid_stream_slot;
}

caf::stream_slot unipath_manager::outbound_path_slot() const noexcept {
  // Work around missing `const` qualifier for `path_slots`.
  auto mutable_this = const_cast<unipath_manager*>(this);
  if (auto vec = mutable_this->out().path_slots(); vec.size() == 1)
    return vec[0];
  else
    return caf::invalid_stream_slot;
}

caf::actor unipath_manager::hdl() const noexcept {
  if (auto& vec = inbound_paths(); vec.size() == 1) {
    return caf::actor_cast<caf::actor>(vec[0]->hdl);
  } else {
    // We only ever have 0 or 1 path. Hence, for_each_path gets called either
    // once or not at all.
    caf::actor result;
    out().for_each_path([&](const caf::outbound_path& x) {
      result = caf::actor_cast<caf::actor>(x.hdl);
    });
    return result;
  }
}

bool unipath_manager::congested(const caf::inbound_path&) const noexcept {
  // This function usually makes sure that stream managers stop processing
  // inputs once they cannot make progress sending. However, the way Broker uses
  // streams deviates from the way "regular" stream managers operate.
  // Essentially, a unipath_manager is a two-way channel. Hence, we cannot ever
  // become "congested" here since inputs and outputs are not correlated.
  return false;
}

void unipath_manager::handle(caf::inbound_path* path,
                             caf::downstream_msg::close& x) {
  closing(true, {});
  super::handle(path, x);
}

void unipath_manager::handle(caf::inbound_path* path,
                             caf::downstream_msg::forced_close& x) {
  closing(false, x.reason);
  super::handle(path, x);
}

void unipath_manager::handle(caf::stream_slots slots,
                             caf::upstream_msg::drop& x) {
  closing(true, {});
  super::handle(slots, x);
}

void unipath_manager::handle(caf::stream_slots slots,
                             caf::upstream_msg::forced_drop& x) {
  closing(false, x.reason);
  super::handle(slots, x);
}

void unipath_manager::closing(bool graceful, const caf::error& reason) {
  if (observer_) {
    observer_->closing(this, graceful, reason);
    observer_ = nullptr;
  }
}

// -- free functions -----------------------------------------------------------

unipath_manager_ptr make_data_source(central_dispatcher* dispatcher) {
  using impl_t = unipath_manager_in<data_message>;
  return caf::make_counted<impl_t>(dispatcher, nullptr);
}

unipath_manager_ptr make_command_source(central_dispatcher* dispatcher) {
  using impl_t = unipath_manager_in<command_message>;
  return caf::make_counted<impl_t>(dispatcher, nullptr);
}

unipath_manager_ptr make_source(central_dispatcher* dispatcher,
                                caf::stream<data_message> in) {
  auto mgr = make_data_source(dispatcher);
  mgr->add_unchecked_inbound_path(in);
  return mgr;
}

unipath_manager_ptr make_source(central_dispatcher* dispatcher,
                                caf::stream<command_message> in) {
  auto mgr = make_command_source(dispatcher);
  mgr->add_unchecked_inbound_path(in);
  return mgr;
}

unipath_manager_ptr make_source(central_dispatcher* dispatcher,
                                caf::stream<node_message_content> in) {
  using impl_t = unipath_manager_in<node_message_content>;
  auto mgr = caf::make_counted<impl_t>(dispatcher, nullptr);
  mgr->add_unchecked_inbound_path(in);
  return mgr;
}

unipath_manager_ptr make_data_sink(central_dispatcher* dispatcher,
                                   filter_type filter) {
  using impl_t = unipath_manager_out<data_message>;
  auto ptr = caf::make_counted<impl_t>(dispatcher, nullptr, std::move(filter));
  dispatcher->add(ptr);
  return ptr;
}

unipath_manager_ptr make_command_sink(central_dispatcher* dispatcher,
                                      filter_type filter) {
  using impl_t = unipath_manager_out<command_message>;
  auto ptr = caf::make_counted<impl_t>(dispatcher, nullptr, std::move(filter));
  dispatcher->add(ptr);
  return ptr;
}

unipath_manager_ptr make_peer_manager(central_dispatcher* dispatcher,
                                      unipath_manager::observer* observer) {
  using base_t = unipath_manager_out<node_message>;
  using impl_t = unipath_manager_in<node_message, base_t>;
  auto ptr = caf::make_counted<impl_t>(dispatcher, observer);
  ptr->block_inputs();
  return ptr;
}

} // namespace broker::detail
