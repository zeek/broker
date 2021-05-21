#define SUITE detail.flow

// #include "broker/detail/flow.hh"

#include "test.hh"

#include "broker/detail/overload.hh"
#include "caf/uuid.hpp"

#include <memory>

using namespace broker;

namespace flow {

/// The subscriber owns the subscription object and only the subscriber may call
/// member functions on its subscription. When destroyed, the subscription
/// automatically calls `cancel()` if the owner did not call it previously.
class subscription {
public:
  virtual ~subscription() {
    // nop
  }
  virtual void cancel() = 0;
  virtual void request(size_t n) = 0;
};

/// A smart pointer for storing a subscription in the subscriber.
using subscription_ptr = std::unique_ptr<subscription>;

template <class T>
class subscriber : public virtual caf::ref_counted {
public:
  virtual void on_complete() = 0;
  virtual void on_error(const caf::error& what) = 0;
  virtual void on_next(T item) = 0;
  virtual void on_subscribe(subscription_ptr subscription) = 0;
};

template <class T>
using subscriber_ptr = caf::intrusive_ptr<subscriber<T>>;

template <class T>
class publisher : public virtual caf::ref_counted {
public:
  virtual ~publisher() {
    // nop
  }

  virtual void subscribe(subscriber_ptr<T> subscriber) = 0;

  template <class F>
  void subscribe(F on_next) {
    class impl : public subscriber<T> {
    public:
      explicit impl(F f) : f_(std::move(f)) {
        // nop
      }
      void on_complete() override {
        // nop
      }
      void on_error(const caf::error&) override {
        // nop
      }
      void on_next(T item) override {
        f_(std::move(item));
        subscription_->request(1);
      }
      void on_subscribe(subscription_ptr subscription) override {
        subscription_ = std::move(subscription);
        subscription_->request(1);
      }
    private:
      F f_;
      subscription_ptr subscription_;
    };
    subscriber_ptr<T> ptr = caf::make_counted<impl>(std::move(on_next));
    subscribe(std::move(ptr));
  }
};

template <class T>
using publisher_ptr = caf::intrusive_ptr<publisher<T>>;

template <class In, class Out>
class processor : public subscriber<In>, public publisher<Out> {
public:
};

} // namespace flow

namespace ng {

enum class topic_id : int64_t {};

template <class Inspector>
bool inspect(Inspector& f, topic_id id) {
  auto get = [&id] { return static_cast<int64_t>(id); };
  auto set = [&id](int64_t value) { id = static_cast<topic_id>(value); };
  return f.apply(get, set);
}

class uuid_multipath {};

class payload : public caf::ref_counted {
public:
  enum class type {
    data,
    command,
  };

  payload(type t, caf::byte_buffer buf)
    : content_type_(t), content_(std::move(buf)) {
    // nop
  }

  type content_type() const noexcept {
    return content_type_;
  }

  caf::span<const caf::byte> content() const noexcept {
    return content_;
  }

private:
  type content_type_;
  caf::byte_buffer content_;
};

template <class>
struct payload_type_oracle;

template <>
struct payload_type_oracle<data_message> {
  static constexpr payload::type value = payload::type::data;
};

template <>
struct payload_type_oracle<command_message> {
  static constexpr payload::type value = payload::type::command;
};

template <class T>
constexpr payload::type payload_type_v = payload_type_oracle<T>::value;

using payload_ptr = caf::intrusive_ptr<payload>;

class source : public flow::subscriber<data_message>,
               public flow::subscriber<command_message> {
public:
};

using tagged_payload = std::pair<topic_id, payload_ptr>;

namespace {

std::mutex tr_mtx_;
topic_id tr_next_id_;
std::unordered_map<topic, topic_id> tr_id_by_topic_;
std::unordered_map<topic_id, topic> tr_topic_by_id_;

topic_id tr_next_id() {
  auto result = tr_next_id_;
  tr_next_id_ = static_cast<topic_id>(static_cast<int64_t>(result) + 1);
  return result;
}

} // namespace

class topic_registry {
public:
  static topic_id id_of(const topic& what) {
    std::unique_lock guard{tr_mtx_};
    if (auto i = tr_id_by_topic_.find(what); i != tr_id_by_topic_.end()) {
      return i->second;
    } else {
      auto id = tr_next_id();
      tr_id_by_topic_.emplace(what, id);
      tr_topic_by_id_.emplace(id, what);
      return id;
    }
  }

  static topic value_of(topic_id what) {
    std::unique_lock guard{tr_mtx_};
    if (auto i = tr_topic_by_id_.find(what); i != tr_topic_by_id_.end()) {
      return i->second;
    } else {
      throw std::logic_error("invalid ID in topic lookup");
    }
  }

private:
  topic_registry();
};

class encoder : public caf::ref_counted {
public:
  using output_type = tagged_payload;
  using subscriber_type = flow::subscriber<output_type>;
  using subscriber_ptr = caf::intrusive_ptr<subscriber_type>;
  using lock_type = std::unique_lock<std::mutex>;

  struct subscribe_event {
    subscriber_ptr who;
    topic what;
  };

  struct unsubscribe_event {
    subscriber_ptr who;
    topic_id what;
  };

  struct credit_event {
    subscriber_ptr who;
    topic_id what;
    size_t amount;
  };

  using event_type
    = std::variant<data_message, command_message, subscribe_event,
                   unsubscribe_event, credit_event>;

  void subscribe(subscriber_type* subscriber, const topic& what) {
    push(subscribe_event{subscriber, what});
  }

private:
  void unsubscribe(subscriber_type* subscriber, topic_id what) {
    push(unsubscribe_event{subscriber, what});
  }

  void request(subscriber_type* subscriber, topic_id what, size_t n) {
    push(credit_event{subscriber, what, n});
  }

  class subscription_impl;

  friend class subscription_impl;

  class subscription_impl : public flow::subscription {
    public:
      subscription_impl(encoder* src, subscriber_ptr snk, topic_id what)
        : src_(src), snk_(std::move(snk)), what_(what) {
        // nop
      }

      ~subscription_impl() {
        if (src_)
          cancel();
      }

      void reset() {
        src_.reset();
        snk_.reset();
      }

      void cancel() override {
        src_->unsubscribe(snk_.get(), what_);
        reset();
      }

      void request(size_t n) override {
        src_->request(snk_.get(), what_, n);
      }

    private:
      caf::intrusive_ptr<encoder> src_;
      subscriber_ptr snk_;
      topic_id what_;
  };

  struct subscriber_state {
    bool active = false;
    size_t credit = 0;
  };

  template <class T>
  void push(T&& event) {
    lock_type guard{queue_mtx_};
    queue_.emplace_back(std::forward<T>(event));
  }

  void run() {
    lock_type guard{queue_mtx_};
    for (;;) {
      if (queue_.empty()) {
        if (queue_closed_)
          return;
        else
          queue_cv_.wait(guard);
      } else {
        auto event = std::move(queue_.front());
        queue_.pop_front();
        guard.unlock();
        handle(event);
        guard.lock();
      }
    }
  }

  void handle(event_type& event) {
    auto f = broker::detail::make_overload(
      [this](auto& msg) {
        caf::byte_buffer buf;
        caf::binary_serializer sink{nullptr, buf};
        using msg_type = std::decay_t<decltype(msg)>;
        static_assert(std::is_same_v<msg_type, data_message>
                      || std::is_same_v<msg_type, command_message>);
        auto& [dst, content] = msg.data();
        auto id = topic_registry::id_of(dst);
        if (auto i = credit_.find(id); i != credit_.end()) {
          if (sink.apply(content)) {
            // ship(i->second, {id, {payload_type_v<msg_type>, std::move(buf)}});
          }
        } else {
          // Drop message: no subscribers.
        }
      },
      [this](subscribe_event& ev) {
        // auto& [who, what] = ev;
        // auto what_id = topic_registry::id_of(what);
        // auto& smap = subscribers_[what_id];
        // if (smap.emplace(who, subscriber_state{}).second) {
        //   auto ack = std::make_unique<subscription_impl>(this, who, what_id);
        //   who->on_subscribe(std::move(ack));
        // } else {
        //   // Ignore repeated subscriptions.
        // }
      },
      [this](unsubscribe_event& ev) {
        // auto& [who, what] = ev;
        // if (auto i = subscribers_.find(what); i != subscribers_.end()) {
        //   auto& smap = i->second;
        //   if (auto j = smap.find(who); j != smap.end()) {
        //     smap.erase(j);
        //     if (!smap.empty()) {
        //       refresh_credit(what);
        //     } else {
        //       subscribers_.erase(i);
        //       credit_.erase(what);
        //       on_credit_change();
        //     }
        //   }
        // }
      },
      [this](credit_event& ev) {
        // auto& [who, what, amount] = ev;
        // if (auto i = subscribers_.find(what); i != subscribers_.end()) {
        //   auto& smap = i->second;
        //   if (auto j = smap.find(who); j != smap.end()) {
        //     auto& state = j->second;
        //     if (state.active) {
        //       state.credit += amount;
        //     } else {
        //       assert(state.credit == 0);
        //       state.active = true;
        //       state.credit = amount;
        //     }
        //     refresh_credit(what);
        //   }
        // }
      });
    std::visit(f, event);
  }

  void ship(size_t& credit, output_type&& what) {
    // if (credit > 0) {
    //   --credit;
    //   auto [first, last] = subscribers_.equal_range(what.first);
    //   std::for_each(first, last, [this, &what](auto& kvp) {
    //     auto& state = kvp.second;
    //     if (state.active) {
    //       assert(state.credit > 0);
    //       --state.credit;
    //       state.ptr->on_next(what);
    //     }
    //   });
    // } else {
    //   auto& buf = stalled_outputs_[what.first];
    //   buf.emplace_back(std::move(what));
    // }
  }

  void on_credit_change() {
    // TODO: implement me
  }

  void refresh_credit(topic_id ) {
    // TODO: implement me
  }

  using subscriber_map = std::unordered_map<subscriber_ptr, subscriber_state>;

  std::unordered_map<topic_id, size_t> credit_;
  std::unordered_map<topic_id, subscriber_map> subscribers_;
  std::unordered_map<topic_id, std::vector<output_type>> stalled_outputs_;

  std::mutex queue_mtx_;
  bool queue_closed_ = false;
  std::condition_variable queue_cv_;
  std::deque<event_type> queue_;
};

class dispatcher {
public:
  /// Adds a new subscriber if possible for receiving events to any active
  /// topic.
  void subscribe(flow::subscriber<tagged_payload>* subscriber);
};

class forwarder : public flow::subscriber<tagged_payload> {
public:
};

class peer_sink : public flow::subscriber<int> {
public:
  virtual void on_path_update(uuid_multipath) = 0;
};

using peer_sink_ptr = caf::intrusive_ptr<peer_sink>;

class peer_sink_factory : public caf::ref_counted {
public:
  ~peer_sink_factory() override {
    // nop
  }

  virtual peer_sink_ptr make(topic_id id, topic dst) = 0;
};

using peer_sink_factory_ptr = caf::intrusive_ptr<peer_sink_factory>;

class core : public caf::ref_counted {
public:
  using lock_type = std::unique_lock<std::mutex>;

  class subscriber_impl;

  friend class subscriber_impl;

  core() : next_id_(static_cast<topic_id>(0)) {
    // nop
  }

  /// @thread-safe
  flow::subscriber_ptr<int> open(const topic& dst);

  /// @thread-safe
  bool add(caf::uuid peer, peer_sink_factory_ptr factory);

  /// @thread-safe
  void drop(caf::uuid peer);

  /// @thread-safe
  bool add_or_update_path(caf::uuid peer, std::vector<caf::uuid> path,
                          alm::vector_timestamp ts);

private:
  topic_id next_id() {
    auto result = next_id_;
    next_id_ = static_cast<topic_id>(static_cast<int64_t>(result) + 1);
    return result;
  }

  mutable std::mutex mtx_;
  topic_id next_id_;
  std::unordered_map<topic, topic_id> flows_;
  std::unordered_map<caf::uuid, peer_sink_factory_ptr> peers_;
};

using core_ptr = caf::intrusive_ptr<core>;

class core::subscriber_impl : public flow::subscriber<int> {
public:
  subscriber_impl(core* parent, topic_id id) : parent_(parent), id_(id) {
  }

  void on_complete() override {
  }

  void on_error(const caf::error& what) override {
  }

  void on_next(int item) override {
  }

  void on_subscribe(flow::subscription_ptr subscription) override {
  }

private:
  core_ptr parent_;
  topic_id id_;
};

flow::subscriber_ptr<int> core::open(const topic& dst) {
  topic_id id;
  {
    lock_type guard{mtx_};
    if (auto i = flows_.find(dst); i != flows_.end()) {
      id = i->second;
    } else {
      id = next_id();
      flows_.emplace(dst, id);
    }
  }
  return caf::make_counted<subscriber_impl>(this, id);
}

class peer : public peer_sink_factory {
public:
  static constexpr size_t max_queue_size = 256;

  class sink;

  friend class sink;

  using lock_type = std::unique_lock<std::mutex>;

  struct flow_state {
    explicit flow_state(topic target) : target(std::move(target)) {
      // nop
    }

    topic target;
    uuid_multipath path;
    std::deque<int> queue;
    bool closed = false;
    flow::subscription_ptr subscription;

    void push(caf::span<const int> items) {
      queue.insert(queue.end(), items.begin(), items.end());
    }
  };

  peer_sink_ptr make(topic_id id, topic dst) override;

private:
  // -- interface for the sink -------------------------------------------------

  void finalize(topic_id id) {
    lock_type guard{mtx_};
  }

  void abort(topic_id id, const caf::error&) {
    finalize(id);
  }

  void push(topic_id id, caf::span<const int> items) {
    // flow::subscription_ptr sub;
    // {
    //   lock_type guard{mtx_};
    //   if (auto i = flows_.find(id); i != flows_.end()) {
    //     sub = i->second.subscription;
    //     i->second.push(items);
    //   }
    // }
    // if (sub)
    //   sub->request(items.size());
  }

  void add(topic_id id, flow::subscription_ptr subscription) {
    // lock_type guard{mtx_};
    // if (auto i = flows_.find(id); i != flows_.end()) {
    //   i->second.subscription = subscription;
    //   guard.unlock();
    //   subscription->request(max_queue_size);
    // } else {
    //   guard.unlock();
    //   subscription->cancel();
    // }
  }

  void update_path(topic_id id, uuid_multipath path) {
    lock_type guard{mtx_};
    if (auto i = flows_.find(id); i != flows_.end())
      i->second.path = std::move(path);
  }

  // -- member variables -------------------------------------------------------

  mutable std::mutex mtx_;
  std::unordered_map<topic_id, flow_state> flows_;
};

class peer::sink : public peer_sink {
public:
  sink(peer* parent, topic_id id) : parent_(parent), id_(id) {
    // nop
  }

  void on_complete() override {
    parent_->finalize(id_);
  }

  void on_error(const caf::error& what) override {
    parent_->abort(id_, what);
  }

  void on_next(int items) override {
    // parent_->push(id_, items);
  }

  void on_subscribe(flow::subscription_ptr subscription) override {
    parent_->add(id_, std::move(subscription));
  }

  void on_path_update(uuid_multipath path) override {
    parent_->update_path(id_, std::move(path));
  }

private:
  caf::intrusive_ptr<peer> parent_;
  topic_id id_;
};

peer_sink_ptr peer::make(topic_id id, topic dst) {
  lock_type guard{mtx_};
  auto added = flows_.emplace(id, flow_state{std::move(dst)}).second;
  guard.unlock();
  if (added)
    return caf::make_counted<sink>(this, id);
  else
    return nullptr;
}

} // namespace ng

namespace {

struct fixture {

};

} // namespace

namespace flow {

using caf::stream_slot;

template <class T>
class message_adapter : public subscriber<T> {
public:
  message_adapter(caf::actor hdl, caf::stream_slot id)
    : hdl_(std::move(hdl)), id_(id) {
    // nop
  }

  void on_complete() {
    caf::anon_send(hdl_, id_, caf::close_atom_v);
  }

  void on_error(const caf::error& what) {
    caf::anon_send(hdl_, id_, what);
  }

  void on_next(T item) {
    caf::anon_send(hdl_, id_, std::move(item));
  }

  void on_subscribe(subscription_ptr subscription) {
    subscription_ = std::move(subscription);
  }

private:
  caf::actor hdl_;
  caf::stream_slot id_;
  subscription_ptr subscription_;
};

template <class T>
struct buffered_merger : public publisher<T> {
public:
  using super = publisher<T>;

  class subscriber_impl;

  static constexpr size_t buffer_size = 256;

  class subscriber_state {
    subscription_ptr subscription;
    // TODO: padding to avoid false sharing.
    std::mutex mtx;
    std::deque<T> buf;
    bool buf_closed = false;
  };

  using list_type = std::list<subscriber_state>;

  using iterator = typename list_type::iterator;

  friend class subscriber_impl;

  buffered_merger() {
    pos_ = subscribers_.end();
  }

  class subscriber_impl : public subscriber<T> {
  public:
    subscriber_impl(buffered_merger* parent, iterator pos)
      : parent_(parent), pos_(pos) {
      // nop
    }

    ~subscriber_impl() override {
      // nop
    }

    void on_complete() override {
      std::unique_lock<std::mutex> guard{pos->mtx};
      pos->buf_closed = true;
    }

    void on_error(const caf::error&) override {
      std::unique_lock<std::mutex> guard{pos->mtx};
      pos->buf_closed = true;
    }

    void on_next(T item) override {
      bool was_empty = false;
      {
        std::unique_lock<std::mutex> guard{pos->mtx};
        if (pos->buf.empty())
          was_empty = true;
        pos->buf.emplace_back(std::move(item));
      }
      if (was_empty) {
        std::unique_lock<std::mutex> guard{parent_->mtx_};
        parent_->try_push(guard);
      }
    }

    void on_subscribe(subscription_ptr subscription) override {
      auto raw_ptr = subscription.get();
      {
        std::unique_lock<std::mutex> guard{pos->mtx};
        pos->subscription = std::move(subscription);
      }
      raw_ptr->request(buffer_size);
    }

  private:
    caf::intrusive_ptr<buffered_merger> parent_;
    iterator pos_;
  };

  subscriber_ptr<T> add_source() {
    subscriber_list tmp;
    tmp.emplace_back();
    auto pos = tmp.begin();
    {
      std::unique_lock<std::mutex> guard{mtx_};
      subscribers_.splice(std::move(tmp));
    }
    return caf::make_counted<subscriber_impl>(this, pos);
  }

  void subscribe(subscriber_ptr<T> subscriber) override {
  }

private:
  size_t pull(iterator pos, size_t n, std::deque<T>& buf) {
    std::unique_lock<std::mutex> guard{pos->mtx};
    auto m = std::min(n, pos->buf.size());
    if (m > 0) {
      auto first = pos->buf.begin();
      auto last = first + m;
      buf.insert(buf.end(), std::make_move_iterator(first),
                 std::make_move_iterator(last));
      pos->buf.erase(first, last);
    }
    return m;
  }

  void try_push() {
    std::deque<T> chunk;
    if (credit_ > 0 ) {
      if (auto s = sources_.size(); s == 0) {
        // nop
      } else if (s == 1) {
        pull(sources_.begin(), credit_, chunk);
      } else {
        auto e = sources_.end();
        auto i = pos_ != e ? pos_ + 1 : sources_.begin();
      }
      if (!chunk.empty()) {
        for (auto& item : chunk)
          sink_->next_item(std::move(item));
        credit_ -= chunk.size();
      }
    }
  }

  std::mutex mtx_;
  subscriber_list sources_;
  iterator pos_;
  size_t credit_ = 0;
  subscriber_ptr sink_;

  stream_slot next_slot() {
    // TODO: this overflows eventually, add some slot reclamation logic.
    std::unique_lock<std::mutex> guard{mtx_};
    return next_slot_++;
  }

  virtual void on_complete(stream_slot) = 0;
  virtual void on_error(stream_slot, const caf::error& what) = 0;
  virtual void on_next(stream_slot, const T& item) = 0;
  virtual void on_subscribe(stream_slot, subscription_ptr subscription) = 0;
};

template <class T, class Container, class OnCompletion>
class collector : public subscriber<T> {
public:
  void on_complete() override {
    finalizer_(buf_);
  }

  void on_error(const caf::error& what) override {
    finalizer_(buf_, what);
  }

  void on_next(const T& item) override {
    buf_.emplace_back(item);
    subscription_->request(1);
  }

  void on_subscribe(subscription_ptr subscription) override {
    subscription_ = std::move(subscription);
    subscription_->request(1);
  }

private:
  Container buf_;
  subscription_ptr subscription_;
  OnCompletion finalizer_;
};

template <class Container>
class iterable_publisher : public publisher<typename Container::value_type> {
public:
  using value_type = typename Container::value_type;

  using const_iterator_type = typename Container::const_iterator;

  explicit iterable_publisher(Container&& values) : values_(std::move(values)) {
    // nop
  }

  void subscribe(subscriber_ptr<value_type> subscriber) override {
    class impl : public subscription {
      public:
        impl(subscriber_ptr<value_type> consumer, const Container& xs,
             publisher<value_type>* ptr)
          : consumer_(std::move(consumer)),
            i_(xs.begin()),
            e_(xs.end()),
            parent_(ptr) {
          // nop
        }

      void request(size_t n) override {
        // This somewhat convoluted way of implementing the loop makes sure that
        // we don't blow up the stack if the subscriber calls request() inside
        // on_next().
        if (credit_ > 0) {
          credit_ += n;
        } else {
          credit_ = n;
          while (credit_ > 0) {
            if (i_ != e_) {
              consumer_->on_next(*i_++);
            } else {
              consumer_->on_complete();
              return;
            }
            --credit_;
          }
        }
      }

      void cancel() override {
        credit_ = 0;
      }

    private:
      // Holding a reference keeps the container alive and our iterators valid.
      subscriber_ptr<value_type> consumer_;
      const_iterator_type i_;
      const_iterator_type e_;
      publisher_ptr<value_type> parent_;
      size_t credit_ = 0;
    };
    auto sub = std::make_unique<impl>(subscriber, values_, this);
    subscriber->on_subscribe(std::move(sub));
  }

private:
  Container values_;
};

template <class Container>
publisher_ptr<typename Container::value_type> from_iterable(Container xs) {
  using impl = iterable_publisher<Container>;
  return caf::make_counted<impl>(std::move(xs));
}

} // namespace flow

FIXTURE_SCOPE(flow_tests, fixture)

TEST(todo) {
  auto observable = flow::from_iterable(std::vector<int>{1, 2, 3});
  observable->subscribe([](int x) { MESSAGE(x); });
}

FIXTURE_SCOPE_END()
