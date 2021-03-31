#include "broker/detail/unipath_manager.hh"

#include <type_traits>

#include <caf/actor_system_config.hpp>
#include <caf/downstream_manager.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/outbound_path.hpp>
#include <caf/scheduled_actor.hpp>
#include <caf/settings.hpp>
#include <caf/typed_message_view.hpp>

#include "broker/alm/stream_transport.hh"
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

// A downstream manager with at most one outbound path.
template <class T>
class unipath_downstream : public caf::downstream_manager {
public:
  using super = downstream_manager;

  using output_type = T;

  using buffer_type = std::deque<output_type>;

  using unique_path_ptr = std::unique_ptr<caf::outbound_path>;

  using super::super;

  void enqueue(const T& msg) {
    if (path_) {
      if constexpr (std::is_same<T, node_message>::value) {
        cache_.emplace_back(msg);
      } else {
        prefix_matcher accept;
        if (accept(filter_, msg))
          cache_.emplace_back(msg);
      }
    } else {
      BROKER_DEBUG("no path available");
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
};

template <class Base>
class unipath_sink_impl : public Base {
public:
  static_assert(std::is_base_of<unipath_manager, Base>::value);

  using super = Base;

  using message_type = typename super::message_type;

  unipath_sink_impl(central_dispatcher* dispatcher,
                      unipath_manager::observer* observer)
    : super(dispatcher, observer), out_(this) {
    // nop
  }

  template <class Filter>
  unipath_sink_impl(central_dispatcher* dispatcher,
                      unipath_manager::observer* observer, Filter&& filter)
    : unipath_sink_impl(dispatcher, observer) {
    out_.filter_ = std::forward<Filter>(filter);
  }

  void enqueue(const message_type& msg) override {
    BROKER_TRACE(BROKER_ARG(msg));
    out_.enqueue(msg);
  }

  filter_type filter() const override {
    return out_.filter_;
  }

  void filter(filter_type new_filter) override {
    out_.filter_ = std::move(new_filter);
  }

  bool accepts(const topic&t) const noexcept override {
    prefix_matcher accept;
    return accept(out_.filter_, t);
  }

  caf::type_id_t message_type_id() const noexcept override {
    return caf::type_id_v<message_type>;
  }

  caf::downstream_manager& out() override {
    return out_;
  }

  bool done() const override {
    auto open_paths = out_.num_paths()
                      + this->pending_handshakes_
                      + this->inbound_paths_.size();
    return open_paths == 0
           || ((!this->running() || super::dispatcher_->tearing_down())
               && this->inbound_paths_idle()
               && out_.clean());
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
      if constexpr (std::is_same<Base, peer_manager>::value) {
        if (!this->handshake_.handle_ack_open_msg()) {
          auto reason = this->handshake_.err;
          this->closing(false, reason);
          return false;
        }
        if (this->handshake_.done())
          this->downstream_connected(caf::actor_cast<caf::actor>(rebind_to));
      }
      return true;
    } else {
      BROKER_ERROR("unipath manager failed to process ack_open!");
      this->closing(false, caf::sec::runtime_error);
      return false;
    }
  }

protected:
  unipath_downstream<message_type> out_;
};

} // namespace

namespace {

template <class T>
struct pending_buf {
  std::vector<T> xs;

  void add(T&& x) {
    if (!x.unique())
      xs.emplace_back(std::move(x));
  }

  void drop_shipped() {
    auto is_shipped = [](const T& msg) { return msg.unique(); };
    xs.erase(std::remove_if(xs.begin(), xs.end(), is_shipped), xs.end());
  }

  [[nodiscard]] size_t size() const noexcept {
    return xs.size();
  }
};

template <>
struct pending_buf<node_message_content> {
  pending_buf<data_message> data_msgs;
  pending_buf<command_message> command_msgs;

  void add(node_message_content&& x) {
    if (is_data_message(x))
      data_msgs.add(std::move(get_data_message(x)));
    else
      command_msgs.add(std::move(get_command_message(x)));
  }

  void drop_shipped() {
    data_msgs.drop_shipped();
    command_msgs.drop_shipped();
  }

  [[nodiscard]] size_t size() const noexcept {
    return data_msgs.size() + command_msgs.size();
  }
};

template <>
struct pending_buf<node_message> : pending_buf<node_message_content> {
  // nop
};

template <class T, class Base = unipath_source>
class unipath_manager_source_impl : public Base {
public:
  using super = Base;

  template <class... Ts>
  explicit unipath_manager_source_impl(central_dispatcher* dispatcher,
                              unipath_manager::observer* observer, Ts&&... xs)
    : super(dispatcher, observer, std::forward<Ts>(xs)...) {
    auto sptr = super::self();
    auto& cfg = sptr->system().config();
  }

  void unblock_inputs() override {
    if (!blocked_messages_.empty()) {
      auto sptr = this->self();
      auto& bs = sptr->bhvr_stack();
      if (bs.empty()) {
        BROKER_ERROR("failed to unblock inputs: actor has no behavior");
      } else {
        auto bhvr = bs.back();
        for (auto& msg : blocked_messages_) {
          if (auto res = bhvr(msg); !res) {
            BROKER_ERROR("failed to unblock inputs: no match for" << msg);
          } else if (!res->empty()) {
            BROKER_ERROR("peer tried to respond to an unblocked input with"
                         << *res);
          }
        }
      }
      blocked_messages_.clear();
    }
    if (!blocked_batches_.empty()) {
      for (auto& batch : blocked_batches_)
        handle(nullptr, batch);
      blocked_batches_.clear();
    }
  }

  void add_blocked_input(caf::message msg) override {
    BROKER_ASSERT(this->blocks_inputs());
    blocked_messages_.emplace_back(std::move(msg));
  }

  caf::type_id_t message_type_id() const noexcept override {
    return caf::type_id_v<T>;
  }

  bool idle() const noexcept override {
    if constexpr (std::is_same<Base, unipath_source>::value) {
      return super::idle();
    } else {
      // The difference from the default implementation is that we do *not*
      // check for out_.stalled(). This is because we limit credit by in-flights
      // from this manager rather than by available credit downstream.
      return this->out_.clean() && this->inbound_paths_idle();
    }
  }

  using super::handle;

  void handle(caf::inbound_path*, caf::downstream_msg::batch& b) override {
    BROKER_TRACE(BROKER_ARG(b));
    BROKER_DEBUG(BROKER_ARG2("batch.size", b.xs_size));
    if (this->blocks_inputs()) {
      blocked_batches_.emplace_back(std::move(b));
    } else if (auto view = caf::make_typed_message_view<std::vector<T>>(b.xs)) {
      for (auto& x : get<0>(view)) {
        if constexpr (std::is_same<T, node_message>::value) {
          force_unshared(get<0>(x.unshared()));
          auto content = get_content(x);
          super::dispatcher_->dispatch(std::move(x));
          pending_.add(std::move(content));
        } else {
          force_unshared(x);
          super::dispatcher_->dispatch(x);
          pending_.add(std::move(x));
        }
      }
    } else {
      BROKER_ERROR("received unexpected batch type (dropped)");
    }
  }

  int32_t acquire_credit(caf::inbound_path* in, int32_t desired) override {
    // Limit credit by pending (in-flight) messages from this path.
    pending_.drop_shipped();
    auto total = in->assigned_credit + desired;
    auto used = static_cast<int32_t>(pending_.size());
    return std::max(total - used, int32_t{0});
  }

private:
  std::vector<node_message> buf_;
  std::vector<caf::message> blocked_messages_;
  std::vector<caf::downstream_msg::batch> blocked_batches_;
  pending_buf<T> pending_;
};

} // namespace

// -- unipath_manager ----------------------------------------------------------

unipath_manager::observer::~observer() {
  // nop
}

unipath_manager::unipath_manager(central_dispatcher* dispatcher, observer* obs)
  : super(dispatcher->this_actor()), dispatcher_(dispatcher), observer_(obs) {
  // nop
}

unipath_manager::~unipath_manager() {
  // nop
}

bool unipath_manager::blocks_inputs() const noexcept {
  return false;
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

caf::event_based_actor* unipath_manager::this_actor() noexcept {
  return dispatcher_->this_actor();
}

endpoint_id unipath_manager::this_endpoint() const {
  return dispatcher_->this_endpoint();
}

filter_type unipath_manager::local_filter() const {
  return dispatcher_->local_filter();
}

alm::lamport_timestamp unipath_manager::local_timestamp() const noexcept {
  return dispatcher_->local_timestamp();
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
  super::handle(path, x);
  if (unconnected())
    closing(true, {});
}

void unipath_manager::handle(caf::inbound_path* path,
                             caf::downstream_msg::forced_close& x) {
  closing(false, x.reason);
  super::handle(path, x);
}

void unipath_manager::handle(caf::stream_slots slots,
                             caf::upstream_msg::drop& x) {
  super::handle(slots, x);
  if (unconnected())
    closing(true, {});
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

void unipath_manager::downstream_connected(caf::actor hdl) {
  if (observer_)
    observer_->downstream_connected(this, std::move(hdl));
}

// -- data_sink ----------------------------------------------------------------

unipath_data_sink::~unipath_data_sink() {
  // nop
}

unipath_manager::derived_pointer unipath_data_sink::derived_ptr() noexcept {
  return this;
}

unipath_data_sink_ptr make_unipath_data_sink(central_dispatcher* dispatcher,
                                             filter_type filter) {
  using impl_t = unipath_sink_impl<unipath_data_sink>;
  return caf::make_counted<impl_t>(dispatcher, nullptr, std::move(filter));
}

// -- command_sink ----------------------------------------------------------------

unipath_command_sink::~unipath_command_sink() {
  // nop
}

unipath_manager::derived_pointer unipath_command_sink::derived_ptr() noexcept {
  return this;
}

unipath_command_sink_ptr
make_unipath_command_sink(central_dispatcher* dispatcher, filter_type filter) {
  using impl_t = unipath_sink_impl<unipath_command_sink>;
  return caf::make_counted<impl_t>(dispatcher, nullptr, std::move(filter));
}

// -- peer_manager -------------------------------------------------------------

peer_manager::peer_manager(central_dispatcher* cp, observer* obs)
  : super(cp, obs), handshake_(this) {
  // nop
}

peer_manager::~peer_manager() {
  // nop
}

bool peer_manager::handshake_completed() const noexcept {
  return handshake_.done();
}

bool peer_manager::blocks_inputs() const noexcept {
  return !handshake_completed();
}

void peer_manager::handshake_failed(error reason) {
  if (observer_)
    observer_->abort_handshake(this);
}

bool peer_manager::finalize_handshake() {
  BROKER_TRACE("");
  if (observer_) {
    if (observer_->finalize_handshake(this)) {
      unblock_inputs();
      return true;
    } else {
      return false;
    }
  } else {
    BROKER_ERROR("cannot finalize a peer_manager without an observer");
    return false;
  }
}

void peer_manager::closing(bool graceful, const caf::error& reason) {
  if (!handshake_completed())
    handshake_.fail(ec::peer_disconnect_during_handshake);
  super::closing(graceful, reason);
}

unipath_manager::derived_pointer peer_manager::derived_ptr() noexcept {
  return this;
}

peer_manager_ptr make_peer_manager(central_dispatcher* dispatcher,
                                   unipath_manager::observer* observer) {
  using base_t = unipath_sink_impl<peer_manager>;
  using impl_t = unipath_manager_source_impl<node_message, base_t>;
  return caf::make_counted<impl_t>(dispatcher, observer);
}

peer_manager_ptr make_peer_manager(alm::stream_transport* transport) {
  return make_peer_manager(transport, transport);
}

// -- unipath_source -----------------------------------------------------------

unipath_source::~unipath_source() {
  // nop
}

unipath_manager::derived_pointer unipath_source::derived_ptr() noexcept {
  return this;
}

filter_type unipath_source::filter() const {
  return {};
}

void unipath_source::filter(filter_type) {
  // nop
}

bool unipath_source::accepts(const topic&) const noexcept {
  return false;
}

caf::downstream_manager& unipath_source::out() {
  return out_;
}

bool unipath_source::done() const {
  return inbound_paths_.empty()
         || (super::dispatcher_->tearing_down() && inbound_paths_idle());
}

bool unipath_source::idle() const noexcept {
  return inbound_paths_idle();
}

unipath_source_ptr make_unipath_source(central_dispatcher* dispatcher,
                                       caf::stream<data_message> in) {
  using impl_t = unipath_manager_source_impl<data_message>;
  auto mgr = caf::make_counted<impl_t>(dispatcher, nullptr);
  mgr->add_unchecked_inbound_path(in);
  return mgr;
}

unipath_source_ptr make_unipath_source(central_dispatcher* dispatcher,
                                       caf::stream<command_message> in) {
  using impl_t = unipath_manager_source_impl<command_message>;
  auto mgr = caf::make_counted<impl_t>(dispatcher, nullptr);
  mgr->add_unchecked_inbound_path(in);
  return mgr;
}

unipath_source_ptr make_unipath_source(central_dispatcher* dispatcher,
                                       caf::stream<node_message_content> in) {
  using impl_t = unipath_manager_source_impl<node_message_content>;
  auto mgr = caf::make_counted<impl_t>(dispatcher, nullptr);
  mgr->add_unchecked_inbound_path(in);
  return mgr;
}

} // namespace broker::detail
