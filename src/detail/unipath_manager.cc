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

namespace broker::detail {

namespace {

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
    BROKER_DEBUG(BROKER_ARG2("ptrs.size", ptrs.size())
                 << BROKER_ARG2("has_path", has_path()));
    if (path_) {
      prefix_matcher accept;
      ptrdiff_t accepted = 0;
      for (auto&& ptr : ptrs) {
        const auto& msg = ptr->msg();
        if (ptr->origin() != super::parent()) {
          if constexpr (std::is_same<T, node_message>::value) {
            if (ptr->ttl() > 1 && accept(filter_, msg)) {
              cache_.emplace_back(make_node_message(msg, ptr->ttl() - 1));
              items_.emplace_back(ptr);
              ++accepted;
            }
          } else if (caf::holds_alternative<T>(msg)) {
            const auto& unboxed = caf::get<T>(msg);
            if (accept(filter_, unboxed)) {
              cache_.emplace_back(unboxed);
              items_.emplace_back(ptr);
              ++accepted;
            }
          }
        }
      }
      return accepted;
    } else {
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
      if (auto delta = old_size - cache_.size(); delta > 0)
        items_.erase(items_.begin(), items_.begin() + delta);
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
};

template <class T>
class unipath_manager_out : public unipath_manager {
public:
  using super = unipath_manager;

  explicit unipath_manager_out(central_dispatcher* dispatcher)
    : super(dispatcher), out_(this) {
    // nop
  }

  template <class Filter>
  unipath_manager_out(central_dispatcher* dispatcher, Filter&& filter)
    : super(dispatcher), out_(this) {
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

  caf::downstream_manager& out() override {
    return out_;
  }

  bool done() const override {
    return !out_.has_path();
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

protected:
  unipath_downstream<T> out_;
};

class unipath_manager_in_only : public unipath_manager {
public:
  using super = unipath_manager;

  unipath_manager_in_only(central_dispatcher* dispatcher)
    : super(dispatcher), out_(this) {
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

  caf::downstream_manager& out() override {
    return out_;
  }

  bool done() const override {
    return !continuous() && inbound_paths_.empty();
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
  explicit unipath_manager_in(central_dispatcher* dispatcher, Ts&&... xs)
    : super(dispatcher, std::forward<Ts>(xs)...) {
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

  bool done() const override {
    if constexpr (std::is_same<Base, unipath_manager_in_only>::value)
      return super::done();
    else
      return !this->continuous() && this->inbound_paths_.empty()
             && this->pending_handshakes_ == 0 && this->out_.clean();
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
    BROKER_DEBUG(BROKER_ARG2("batch.size", x.xs_size));
    if (auto view = caf::make_typed_message_view<std::vector<T>>(x.xs)) {
      auto& vec = get<0>(view);
      if (vec.size() <= stash_->available()) {
        items_.reserve(vec.size());
        for (auto& x : vec) {
          item_ptr ptr;
          if constexpr (std::is_same<T, node_message>::value) {
            auto ttl = std::min(ttl_, x.ttl);
            ptr = stash_->next_item(std::move(x.content), ttl, this);
          } else {
            ptr = stash_->next_item(std::move(x), ttl_, this);
          }
          items_.emplace_back(std::move(ptr));
        }
        super::dispatcher_->enqueue(items_);
        super::dispatcher_->ship();
        items_.clear();
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
};

} // namespace

unipath_manager::unipath_manager(central_dispatcher* dispatcher)
  : super(dispatcher->self()), dispatcher_(dispatcher) {
  // nop
}

unipath_manager::~unipath_manager() {
  // nop
}

unipath_manager_ptr make_data_source(central_dispatcher* dispatcher) {
  using impl_t = unipath_manager_in<data_message>;
  return caf::make_counted<impl_t>(dispatcher);
}

unipath_manager_ptr make_command_source(central_dispatcher* dispatcher) {
  using impl_t = unipath_manager_in<command_message>;
  return caf::make_counted<impl_t>(dispatcher);
}

unipath_manager_ptr make_data_sink(central_dispatcher* dispatcher,
                                   filter_type filter) {
  using impl_t = unipath_manager_out<data_message>;
  auto result = caf::make_counted<impl_t>(dispatcher, std::move(filter));
  dispatcher->add(result);
  return result;
}

unipath_manager_ptr make_command_sink(central_dispatcher* dispatcher,
                                      filter_type filter) {
  using impl_t = unipath_manager_out<command_message>;
  auto result = caf::make_counted<impl_t>(dispatcher, std::move(filter));
  dispatcher->add(result);
  return result;
}

unipath_manager_ptr make_peer_manager(central_dispatcher* dispatcher) {
  using base_t = unipath_manager_out<node_message>;
  using impl_t = unipath_manager_in<node_message, base_t>;
  auto result = caf::make_counted<impl_t>(dispatcher);
  dispatcher->add(result);
  return result;
}

} // namespace broker::detail
