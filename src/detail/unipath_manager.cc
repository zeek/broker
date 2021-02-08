#include "broker/detail/unipath_manager.hh"

#include <type_traits>

#include <caf/actor_system_config.hpp>
#include <caf/downstream_manager.hpp>
#include <caf/downstream_manager_base.hpp>
#include <caf/outbound_path.hpp>
#include <caf/scheduled_actor.hpp>
#include <caf/settings.hpp>
#include <caf/typed_message_view.hpp>

#include "broker/defaults.hh"
#include "broker/detail/assert.hh"
#include "broker/detail/central_dispatcher.hh"
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

// Checks whether a downstream of type T is eligible for inputs of given scope.
template <class T>
[[nodiscard]] constexpr bool is_eligible(item_scope x) noexcept {
  if constexpr (std::is_same<T, data_message>::value
                || std::is_same<T, command_message>::value) {
    // Paths to data_message and command_message receivers always belong to
    // local subscribers.
    return x != item_scope::remote;
  } else {
    // Paths to node_message receivers always forward data to peers.
    static_assert(std::is_same<T, node_message>::value);
    return x != item_scope::local;
  }
}

// Checks whether a downstream of type T is eligible a given message.
template <class T>
[[nodiscard]] bool is_eligible(const node_message& msg) noexcept {
  if constexpr (std::is_same<T, data_message>::value) {
    // Paths to data_message receivers are always local subscribers.
    return is_data_message(msg.content);
  } else if constexpr (std::is_same<T, command_message>::value) {
    // Paths to command_message receivers are also always local subscribers.
    return  is_command_message(msg.content);
  } else {
    // Paths to node_message receivers are always peers.
    static_assert(std::is_same<T, node_message>::value);
    return msg.ttl > 0;
  }
}

// A downstream manager with at most one outbound path.
template <class T>
class unipath_downstream : public caf::downstream_manager_base {
public:
  using super = caf::downstream_manager_base;

  using output_type = T;

  using buffer_type = std::deque<output_type>;

  using unique_path_ptr = std::unique_ptr<caf::outbound_path>;

  unipath_downstream(caf::stream_manager* parent)
    : super(parent, caf::type_id_v<T>) {
    // nop
  }

  ~unipath_downstream() {
    if (!cache_.empty())
      super::dropped_messages(cache_.size());
  }

  bool enqueue(item_scope scope, caf::span<const node_message> messages,
               long pending_handshakes) {
    BROKER_TRACE(BROKER_ARG(scope)
                 << BROKER_ARG(pending_handshakes)
                 << BROKER_ARG2("num-messages", messages.size()));
    if (is_eligible<T>(scope)) {
      prefix_matcher matches_filter;
      auto old_size = cache_.size();
      for (const auto& msg : messages) {
        if (is_eligible<T>(msg) && matches_filter(filter_, msg)) {
          if constexpr (std::is_same<T, data_message>::value) {
            cache_.emplace_back(caf::get<data_message>(msg.content));
          } else if constexpr (std::is_same<T, command_message>::value) {
            cache_.emplace_back(caf::get<command_message>(msg.content));
          } else {
            cache_.emplace_back(msg);
          }
        }
      }
      if (auto added = cache_.size() - old_size; added > 0) {
        super::generated_messages(added);
        if (path_) {
          emit_batches_impl(false);
          return true;
        } else {
          return pending_handshakes != 0;
        }
      }
    }
    return path_ || pending_handshakes != 0;
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
    BROKER_TRACE(BROKER_ARG(x) << BROKER_ARG(reason) << BROKER_ARG(silent));
    if (is_this_slot(x)) {
      super::about_to_erase(path_.get(), silent, &reason);
      path_.reset();
      cache_.clear();
      return true;
    } else {
      return false;
    }
  }

  super::path_ptr path(caf::stream_slot x) noexcept override {
    return is_this_slot(x) ? path_.get() : nullptr;
  }

  void emit_batches_impl(bool forced) {
    auto old_size = cache_.size();
    path_->emit_batches(super::self(), cache_, forced || path_->closing);
    auto new_size = cache_.size();
    if (auto shipped = old_size - new_size; shipped > 0) {
      super::shipped_messages(shipped);
      super::last_send_ = super::self()->now();
    }
  }

  void emit_batches() override {
    if (path_ && !cache_.empty())
      emit_batches_impl(false);
  }

  void force_emit_batches() override {
    if (path_ && !cache_.empty())
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
    BROKER_TRACE(BROKER_ARG(ptr));
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
    BROKER_TRACE(BROKER_ARG(filter));
    out_.filter_ = std::forward<Filter>(filter);
  }

  bool enqueue(const unipath_manager* source, item_scope scope,
               caf::span<const node_message> xs) override {
    if (source != this) {
      return out_.enqueue(scope, xs, pending_handshakes_);
    } else {
      return true;
    }
  }

  filter_type filter() override {
    return out_.filter_;
  }

  void filter(filter_type new_filter) override {
    BROKER_TRACE(BROKER_ARG(new_filter));
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

  bool enqueue(const unipath_manager*, item_scope,
               caf::span<const node_message>) override {
    return false;
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
    if (!std::is_same<T, node_message>::value
        || caf::get_or(cfg, "broker.forward", true)) {
      ttl_ = caf::get_or(cfg, "broker.ttl", defaults::ttl);
    } else {
      // Set TTL to 0 when forwarding was disabled and this manager receives
      // node messages from other peers. This causes all peer managers to drop
      // node messages instead of forwarding them.
      ttl_ = 0;
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
    // The difference from the default implementation is that we do *not* check
    // for out_.stalled(). This is because we limit credit by in-flights from
    // this manager rather than by available credit downstream.
    return this->out_.clean() && this->inbound_paths_idle();
  }

  using super::handle;

  void handle_batch(std::vector<node_message>& xs) {
    auto old_size = pending_.size();
    for (auto& x : xs) {
      if (x.ttl == 0) {
        BROKER_WARNING("received node message with TTL 0: dropped");
        continue;
      }
      // Somewhat hacky, but don't forward data store clone messages.
      auto ttl = ends_with(get_topic(x).string(), topics::clone_suffix.string())
                 ? uint16_t{0}
                 : std::min(ttl_, static_cast<uint16_t>(x.ttl - 1));
      x.ttl = ttl;
      // We are using the reference count as a means to detect whether all
      // receivers have processed the message. Hence, we must make sure that the
      // reference count to the message's content is 1 at this point.
      force_unshared(x);
      pending_.emplace_back(std::move(x));
    }
    if (auto added = pending_.size() - old_size; added > 0) {
      auto ys = caf::make_span(std::addressof(pending_[old_size]), added);
      super::dispatcher_->enqueue(this, item_scope::global, ys);
    }
  }

  template <class MessageType>
  void handle_batch(std::vector<MessageType>& xs) {
    auto old_size = pending_.size();
    for (auto& x : xs) {
      force_unshared(x);
      pending_.emplace_back(make_node_message(std::move(x), ttl_));
    }
    if (auto added = pending_.size() - old_size; added > 0) {
      auto ys = caf::make_span(std::addressof(pending_[old_size]), added);
      super::dispatcher_->enqueue(this, item_scope::remote, ys);
    }
  }

  void handle(caf::inbound_path*, caf::downstream_msg::batch& b) override {
    BROKER_TRACE(BROKER_ARG(b));
    BROKER_DEBUG(BROKER_ARG2("batch.size", b.xs_size)
                 << BROKER_ARG(block_inputs_));
    if (block_inputs_) {
      blocked_batches_.push_back(std::move(b));
    } else if (auto view = caf::make_typed_message_view<std::vector<T>>(b.xs)) {
      handle_batch(get<0>(view));
    } else {
      BROKER_ERROR("received unexpected batch type (dropped)");
    }
  }

  int32_t acquire_credit(caf::inbound_path* in, int32_t desired) override {
    // Drop pending inputs that are no longer referenced by output paths.
    auto is_shipped = [](const node_message& msg) {
      if (is_data_message(msg)) {
        return caf::get<data_message>(msg.content).unique();
      } else {
        return caf::get<command_message>(msg.content).unique();
      }
    };
    pending_.erase(std::remove_if(pending_.begin(), pending_.end(), is_shipped),
                   pending_.end());
    // Limit credit by pending (in-flight) messages from this path.
    auto total = in->assigned_credit + desired;
    auto used = static_cast<int32_t>(pending_.size());
    if (auto delta = total - used; delta > 0)
      return delta;
    else
      return 0;
  }

private:
  uint16_t ttl_;
  bool block_inputs_ = false;
  std::vector<caf::downstream_msg::batch> blocked_batches_;
  std::vector<node_message> pending_;
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
