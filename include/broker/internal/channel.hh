#pragma once

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <deque>
#include <optional>
#include <variant>

#include <caf/actor.hpp>
#include <caf/send.hpp>

#include "broker/error.hh"
#include "broker/internal/logger.hh"
#include "broker/internal/metric_factory.hh"
#include "broker/lamport_timestamp.hh"
#include "broker/none.hh"

namespace broker::internal {

/// A message-driven channel for ensuring reliable and ordered transport over an
/// unreliable and unordered communication layer. A channel connects a single
/// producer with any number of consumers.
template <class Handle, class Payload>
class channel {
public:
  // -- member types: messages from consumers to the producer ------------------

  /// Notifies the producer that a consumer received all events up to a certain
  /// sequence number (including that number). Consumers send the latest ACK
  /// periodically as a keepalive message.
  struct cumulative_ack {
    sequence_number_type seq;

    template <class Inspector>
    friend bool inspect(Inspector& f, cumulative_ack& x) {
      return f.object(x)
        .pretty_name("cumulative_ack")
        .fields(f.field("seq", x.seq));
    }
  };

  /// Notifies the producer that a consumer failed to received some events.
  /// Sending a NACK for the sequence number 0 causes the publisher to re-send
  /// the handshake.
  struct nack {
    std::vector<sequence_number_type> seqs;

    template <class Inspector>
    friend bool inspect(Inspector& f, nack& x) {
      return f.object(x).pretty_name("nack").fields(f.field("seqs", x.seqs));
    }
  };

  // -- member types: messages from the producer to consumers ------------------

  /// Notifies a consumer which is the first sequence number after it started
  /// listening to the producer.
  struct handshake {
    /// The first sequence number a consumer should process and acknowledge.
    sequence_number_type offset;

    /// The interval (in ticks) between heartbeat messages. Allows the consumer
    /// to adjust its timeouts for detecting failed producers.
    tick_interval_type heartbeat_interval;

    /// Maximum number of missed heartbeats before connections time out.
    tick_interval_type connection_timeout;

    template <class Inspector>
    friend bool inspect(Inspector& f, handshake& x) {
      return f.object(x)
        .pretty_name("handshake")
        .fields(f.field("offset", x.offset),
                f.field("heartbeat_interval", x.heartbeat_interval));
    }
  };

  /// Transmits ordered data to a consumer.
  struct event {
    sequence_number_type seq;
    Payload content;

    template <class Inspector>
    friend bool inspect(Inspector& f, event& x) {
      return f.object(x)
        .pretty_name("event") //
        .fields(f.field("seq", x.seq), f.field("content", x.content));
    }
  };

  /// Notifies a consumer that the producer can no longer retransmit an event.
  struct retransmit_failed {
    sequence_number_type seq;

    template <class Inspector>
    friend bool inspect(Inspector& f, retransmit_failed& x) {
      return f.object(x)
        .pretty_name("retransmit_failed")
        .fields(f.field("seq", x.seq));
    }
  };

  /// Notifies all consumers that the master is still alive and what is the
  /// latest sequence number.
  struct heartbeat {
    sequence_number_type seq;

    template <class Inspector>
    friend bool inspect(Inspector& f, heartbeat& x) {
      return f.object(x).pretty_name("heartbeat").fields(f.field("seq", x.seq));
    }
  };

  // -- implementation of the producer -----------------------------------------

  /// Messages sent by the producer.
  using producer_message =
    std::variant<handshake, event, retransmit_failed, heartbeat>;

  struct default_producer_base {};

  /// Produces events (messages) for any number of consumers.
  /// @tparam Backend Hides the underlying (unreliable) communication layer. The
  ///                 backend must provide the following member functions:
  ///                 - `void send(producer*, const Handle&, const T&)` sends a
  ///                   unicast message to a single consumer, where `T` is any
  ///                   type in `producer_message`.
  ///                 - `void broadcast(producer*, const T&)` sends a multicast
  ///                   message to all consumers, where `T` is any type in
  ///                   `producer_message`.
  ///                 - `void drop(producer*, const Handle&, ec)` called to
  ///                   indicate that a consumer got removed by the producer.
  ///                 - `void handshake_completed(producer*, const Handle&)`
  ///                   called to indicate that the producer received an ACK
  template <class Backend, class Base = default_producer_base>
  class producer : public Base {
  public:
    // -- member types ---------------------------------------------------------

    /// Bundles bookkeeping state for a consumer.
    struct path {
      /// Allows the backend to uniquely address this consumer.
      Handle hdl;

      /// The sequence number that was active when adding this consumer.
      sequence_number_type offset;

      /// The sequence number of the last cumulative ACK.
      sequence_number_type acked;

      /// The last time we have received a message on this path.
      lamport_timestamp last_seen;
    };

    using buf_type = std::deque<event>;

    using path_list = std::vector<path>;

    /// Bundles metrics for the producer.
    struct metrics_t {
      /// Keeps track of how many output channels exist.
      caf::telemetry::int_gauge* output_channels = nullptr;

      /// Keeps track of how many messages currently wait for an ACK.
      caf::telemetry::int_gauge* unacknowledged = nullptr;

      /// Keeps track of how many messages were sent (acknowledged) in total.
      caf::telemetry::int_counter* processed = nullptr;

      void init(caf::telemetry::metric_registry& reg, std::string_view name) {
        metric_factory factory{reg};
        output_channels = factory.store.output_channels_instance(name);
        unacknowledged = factory.store.unacknowledged_updates_instance(name);
        processed = factory.store.processed_updates_instance(name);
      }

      void init(caf::actor_system& sys, std::string_view name) {
        init(sys.metrics(), name);
      }

      bool initialized() const noexcept {
        // Either all pointer are null or none.
        return output_channels != nullptr;
      }

      void inc_output_channels() {
        if (output_channels)
          output_channels->inc();
      }

      void dec_output_channels() {
        if (output_channels)
          output_channels->dec();
      }

      void inc_unacknowledged() {
        if (unacknowledged)
          unacknowledged->inc();
      }

      void shipped(int64_t num) {
        if (unacknowledged) {
          unacknowledged->dec(num);
          processed->inc(num);
        }
      }
    };

    // -- constructors, destructors, and assignment operators ------------------

    explicit producer(Backend* backend) : backend_(backend) {
      // nop
    }

    // -- message processing ---------------------------------------------------

    void produce(Payload content) {
      if (paths_.empty())
        return;
      metrics_.inc_unacknowledged();
      ++seq_;
      buf_.emplace_back(event{seq_, std::move(content)});
      last_broadcast_ = tick_;
      backend_->broadcast(this, buf_.back());
    }

    error add(const Handle& hdl) {
      if (find_path(hdl) != paths_.end())
        return ec::consumer_exists;
      BROKER_DEBUG("add" << hdl << "to the channel");
      metrics_.inc_output_channels();
      paths_.emplace_back(path{hdl, seq_, 0, tick_});
      backend_->send(this, hdl, handshake{seq_, heartbeat_interval_});
      return {};
    }

    void trigger_handshakes() {
      for (auto& path : paths_)
        if (path.offset == 0)
          backend_->send(this, path.hdl,
                         handshake{path.offset, heartbeat_interval_});
    }

    void handle_ack(const Handle& hdl, sequence_number_type seq) {
      sequence_number_type acked = seq;
      // Iterate all paths once, fetching minimum acknowledged sequence number
      // and updating the path belonging to `hdl` in one go.
      for (auto& x : paths_) {
        if (x.hdl == hdl) {
          if (x.acked > seq) {
            // A blast from the past. Ignore.
            return;
          }
          x.last_seen = tick_;
          if (x.acked == 0) {
            backend_->handshake_completed(this, hdl);
          } else if (x.acked == seq) {
            // Old news. Stop processing this event, since it won't allow us to
            // clear events from the buffer anyways.
            return;
          }
          x.acked = seq;
        } else {
          acked = std::min(x.acked, acked);
        }
      }
      // Drop events from the buffer if possible.
      auto not_acked = [acked](const event& x) { return x.seq > acked; };
      auto new_begin = std::find_if(buf_.begin(), buf_.end(), not_acked);
      if (auto n = std::distance(buf_.begin(), new_begin); n > 0) {
        metrics_.shipped(n);
        buf_.erase(buf_.begin(), new_begin);
      }
    }

    void handle_nack(const Handle& hdl,
                     const std::vector<sequence_number_type>& seqs) {
      // Sanity checks.
      if (seqs.empty())
        return;
      // Nack 0 implicitly acts as a handshake.
      auto p = find_path(hdl);
      if (p == paths_.end()) {
        if (seqs.size() == 1 && seqs.front() == 0) {
          auto err = add(hdl);
          static_cast<void>(err); // Discard: always default-constructed.
        }
        return;
      }
      // Seqs must be sorted. Everything before the first missing ID is ACKed.
      p->last_seen = tick_;
      if (seqs.size() > 1 && !std::is_sorted(seqs.begin(), seqs.end())) {
        backend_->drop(this, p->hdl, ec::invalid_message);
        paths_.erase(p);
        return;
      }
      auto first = seqs.front();
      if (first == 0) {
        backend_->send(this, hdl, handshake{p->offset, heartbeat_interval_});
        return;
      }
      handle_ack(hdl, first - 1);
      for (auto seq : seqs) {
        if (auto i = find_event(seq); i != buf_.end())
          backend_->send(this, hdl, *i);
        else
          backend_->send(this, hdl, retransmit_failed{seq});
      }
    }

    // -- time-based processing ------------------------------------------------

    void tick() {
      BROKER_TRACE("");
      // Increase local time and send heartbeats.
      ++tick_;
      if (heartbeat_interval_ == 0)
        return;
      if (last_broadcast_ + heartbeat_interval_ == tick_) {
        last_broadcast_ = tick_;
        backend_->broadcast(this, heartbeat{seq_});
      }
      // Check whether any consumer timed out.
      auto timeout = connection_timeout();
      assert(timeout > 0);
      size_t erased_paths = 0;
      for (auto i = paths_.begin(); i != paths_.end();) {
        if (tick_.value - i->last_seen.value >= timeout) {
          BROKER_DEBUG("remove" << i->hdl << "from channel: consumer timeout");
          metrics_.dec_output_channels();
          backend_->drop(this, i->hdl, ec::connection_timeout);
          i = paths_.erase(i);
          ++erased_paths;
        } else {
          ++i;
        }
      }
      // Check whether we can clear some items from the buffer.
      if (paths_.empty()) {
        buf_.clear();
      } else if (erased_paths > 0) {
        auto i = paths_.begin();
        auto acked = i->acked;
        for (++i; i != paths_.end(); ++i)
          if (i->acked < acked)
            acked = i->acked;
        auto not_acked = [acked](const event& x) { return x.seq > acked; };
        auto new_begin = std::find_if(buf_.begin(), buf_.end(), not_acked);
        if (auto n = std::distance(buf_.begin(), new_begin); n > 0) {
          metrics_.shipped(n);
          buf_.erase(buf_.begin(), new_begin);
        }
      }
    }

    // -- properties -----------------------------------------------------------

    auto& backend() noexcept {
      return *backend_;
    }

    const auto& backend() const noexcept {
      return *backend_;
    }

    metrics_t& metrics() noexcept {
      return metrics_;
    }

    const metrics_t& metrics() const noexcept {
      return metrics_;
    }

    auto tick_time() const noexcept {
      return tick_;
    }

    auto seq() const noexcept {
      return seq_;
    }

    auto next_seq() const noexcept {
      return seq_ + 1;
    }

    const auto& buf() const noexcept {
      return buf_;
    }

    const auto& paths() const noexcept {
      return paths_;
    }

    auto heartbeat_interval() const noexcept {
      return heartbeat_interval_;
    }

    void heartbeat_interval(tick_interval_type value) noexcept {
      heartbeat_interval_ = value;
    }

    auto connection_timeout() const noexcept {
      return uint64_t{heartbeat_interval_} * connection_timeout_factor_;
    }

    auto connection_timeout_factor() const noexcept {
      return connection_timeout_factor_;
    }

    void connection_timeout_factor(tick_interval_type value) noexcept {
      connection_timeout_factor_ = value;
    }

    bool idle() const noexcept {
      auto at_head = [seq{seq_}](const path& x) { return x.acked == seq; };
      return std::all_of(paths_.begin(), paths_.end(), at_head);
    }

    /// Checks whether any path was added but not yet acknowledged.
    bool has_pending_paths() const noexcept {
      auto pending = [](const path& x) { return x.acked == 0; };
      return std::any_of(paths_.begin(), paths_.end(), pending);
    }

    // -- path and event lookup ------------------------------------------------

    auto find_path(const Handle& hdl) noexcept {
      auto has_hdl = [&hdl](const path& x) { return x.hdl == hdl; };
      return std::find_if(paths_.begin(), paths_.end(), has_hdl);
    }

    auto find_path(const Handle& hdl) const noexcept {
      auto has_hdl = [&hdl](const path& x) { return x.hdl == hdl; };
      return std::find_if(paths_.begin(), paths_.end(), has_hdl);
    }

    auto find_event(sequence_number_type seq) noexcept {
      auto has_seq = [seq](const event& x) { return x.seq == seq; };
      return std::find_if(buf_.begin(), buf_.end(), has_seq);
    }

  private:
    // -- member variables -----------------------------------------------------

    /// Transmits messages to the consumers.
    Backend* backend_;

    /// Caches pointers to the metric instances.
    metrics_t metrics_;

    /// Monotonically increasing counter (starting at 1) to establish ordering
    /// of messages on this channel. Since we start at 1, the first message we
    /// send is going to have a sequence number of *2*. This enables us to
    /// use 0 on a path to mean "added but we never received an ack yet",
    /// because an ACK cannot have the sequence number 0.
    sequence_number_type seq_ = 1;

    /// Monotonically increasing counter to keep track of time.
    lamport_timestamp tick_;

    /// Stores the last time we've broadcasted something.
    lamport_timestamp last_broadcast_;

    /// Stores outgoing events with their sequence number.
    buf_type buf_;

    /// List of consumers with the last acknowledged sequence number.
    path_list paths_;

    /// Maximum time between to broadcasted messages. When not sending anything
    /// else, insert heartbeats after this amount of time.
    tick_interval_type heartbeat_interval_ = 5;

    /// Factor for computing the timeout for consumers, i.e., after how many
    /// heartbeats of not receiving any message do we assume the consumer no
    /// longer exists.
    tick_interval_type connection_timeout_factor_ = 4;
  };

  // -- implementation of the consumer -----------------------------------------

  /// Messages sent by the consumer.
  using consumer_message = std::variant<cumulative_ack, nack>;

  /// Handles events (messages) from a single producer.
  /// @tparam Backend Hides the underlying (unreliable) communication layer. The
  ///                 backend must provide the following member functions:
  ///                 - `void consume(consumer*, Payload)` process a single
  ///                 event.
  ///                 - `void send(consumer*, T)` sends a message to the
  ///                   producer, where `T` is any type in `consumer_message`.
  ///                 - `error consume_nil(consumer*)` process a lost event. The
  ///                   callback may abort further processing by returning a
  ///                   non-default `error`. In this case, the `consumer`
  ///                   immediately calls `close` with the returned error.
  ///                 - `void close(consumer*, error)` drops this consumer.
  ///                   After calling this function, no further function calls
  ///                   on the consumer are allowed (except calling the
  ///                   destructor).
  template <class Backend>
  class consumer {
  public:
    // -- member types ---------------------------------------------------------

    struct optional_event {
      sequence_number_type seq;
      std::optional<Payload> content;

      explicit optional_event(sequence_number_type seq) : seq(seq) {
        // nop
      }

      template <class T>
      optional_event(sequence_number_type seq, T&& x)
        : seq(seq), content(std::forward<T>(x)) {
        // nop
      }

      template <class Inspector>
      friend bool inspect(Inspector& f, optional_event& x) {
        return f.object(x)
          .pretty_name("optional_event")
          .fields(f.field("seq", x.seq), f.field("content", x.content));
      }
    };

    using buf_type = std::deque<optional_event>;

    /// Bundles metrics for the consumer.
    struct metrics_t {
      /// Keeps track of how many output channels exist.
      caf::telemetry::int_gauge* input_channels = nullptr;

      /// Keeps track of how many messages are currently buffered because they
      /// arrived out-of-order.
      caf::telemetry::int_gauge* out_of_order_updates = nullptr;

      void init(caf::telemetry::metric_registry& reg, std::string_view name) {
        metric_factory mf{reg};
        input_channels = mf.store.input_channels_instance(name);
        out_of_order_updates = mf.store.out_of_order_updates_instance(name);
      }

      void init(caf::actor_system& sys, std::string_view name) {
        init(sys.metrics(), name);
      }

      bool initialized() const noexcept {
        // Either all pointer are null or none.
        return input_channels != nullptr;
      }

      void inc_input_channels() {
        if (input_channels)
          input_channels->inc();
      }

      void dec_input_channels() {
        if (input_channels)
          input_channels->dec();
      }

      void inc_out_of_order_updates() {
        if (out_of_order_updates)
          out_of_order_updates->inc();
      }

      void dec_out_of_order_updates(int64_t n = 1) {
        if (out_of_order_updates)
          out_of_order_updates->dec(n);
      }
    };

    // -- constructors, destructors, and assignment operators ------------------

    explicit consumer(Backend* backend) : backend_(backend) {
      // nop
    }

    // -- message processing ---------------------------------------------------

    /// Initializes the consumer from the settings in the handshake.
    /// @returns `true` if the consumer was initialized, `false` on a repeated
    ///          handshake that got dropped by the consumer.
    bool handle_handshake(Handle producer_hdl, sequence_number_type offset,
                          tick_interval_type heartbeat_interval) {
      BROKER_TRACE(BROKER_ARG(producer_hdl)
                   << BROKER_ARG(offset) << BROKER_ARG(heartbeat_interval));
      if (initialized())
        return false;
      producer_ = std::move(producer_hdl);
      return handle_handshake_impl(offset, heartbeat_interval);
    }

    /// @copydoc handle_handshake
    bool handle_handshake(sequence_number_type offset,
                          tick_interval_type heartbeat_interval) {
      BROKER_TRACE(BROKER_ARG(offset) << BROKER_ARG(heartbeat_interval));
      if (initialized())
        return false;
      return handle_handshake_impl(offset, heartbeat_interval);
    }

    bool handle_handshake_impl(sequence_number_type offset,
                               tick_interval_type heartbeat_interval) {
      BROKER_TRACE(BROKER_ARG(offset) << BROKER_ARG(heartbeat_interval));
      // Initialize state.
      next_seq_ = offset + 1;
      last_seq_ = next_seq_;
      heartbeat_interval_ = heartbeat_interval;
      // Find the first message in the assigned offset and drop any buffered
      // message before that point.
      if (!buf_.empty()) {
        auto pred = [=](const optional_event& x) { return x.seq > offset; };
        auto new_begin = std::find_if(buf_.begin(), buf_.end(), pred);
        if (auto n = std::distance(buf_.begin(), new_begin); n > 0) {
          metrics_.dec_out_of_order_updates(n);
          buf_.erase(buf_.begin(), new_begin);
        }
      }
      // Consume buffered messages if possible and send initial ACK.
      try_consume_buffer();
      send_ack();
      // Update our metric. This counter can only oscillate between 0 and 1.
      metrics_.inc_input_channels();
      return true;
    }

    void handle_heartbeat(sequence_number_type seq) {
      // Do nothing when receiving this before the handshake or if the master
      // did not produce any events yet.
      if (last_seq_ == 0 || seq == 0)
        return;
      if (seq + 1 > last_seq_)
        last_seq_ = seq + 1;
    }

    void handle_event(sequence_number_type seq, Payload payload) {
      BROKER_TRACE(BROKER_ARG(seq) << BROKER_ARG(payload));
      if (next_seq_ == seq) {
        // Process immediately.
        backend_->consume(this, payload);
        bump_seq();
        try_consume_buffer();
      } else if (seq > next_seq_) {
        if (seq > last_seq_)
          last_seq_ = seq;
        // Insert event into buf_: sort by the sequence number, drop duplicates.
        auto pred = [seq](const optional_event& x) { return x.seq >= seq; };
        auto i = std::find_if(buf_.begin(), buf_.end(), pred);
        if (i == buf_.end()) {
          buf_.emplace_back(seq, std::move(payload));
          metrics_.inc_out_of_order_updates();
        } else if (i->seq != seq) {
          metrics_.inc_out_of_order_updates();
          buf_.emplace(i, seq, std::move(payload));
        } else if (!i->content) {
          i->content = std::move(payload);
        }
      }
    }

    void handle_retransmit_failed(sequence_number_type seq) {
      if (next_seq_ == seq) {
        // Process immediately.
        if (auto err = backend_->consume_nil(this)) {
          backend_->close(this, std::move(err));
          reset();
          return;
        }
        bump_seq();
        try_consume_buffer();
      } else if (seq > next_seq_) {
        // Insert event into buf_: sort by the sequence number, drop duplicates.
        auto pred = [seq](const optional_event& x) { return x.seq >= seq; };
        auto i = std::find_if(buf_.begin(), buf_.end(), pred);
        if (i == buf_.end()) {
          buf_.emplace_back(seq);
          metrics_.inc_out_of_order_updates();
        } else if (i->seq != seq) {
          buf_.emplace(i, seq);
          metrics_.inc_out_of_order_updates();
        }
      }
    }

    // -- time-based processing ------------------------------------------------

    void tick() {
      BROKER_TRACE(BROKER_ARG2("next_seq", next_seq_)
                   << BROKER_ARG2("last_seq", last_seq_)
                   << BROKER_ARG2("buf.size", buf().size()));
      ++tick_;
      // Ask for repeated handshake each heartbeat interval when not fully
      // initialized yet.
      if (!initialized()) {
        BROKER_DEBUG("not fully initialized: waiting for producer handshake");
        ++idle_ticks_;
        if (idle_ticks_ >= nack_timeout_) {
          idle_ticks_ = 0;
          backend_->send(this, nack{std::vector<sequence_number_type>{0}});
        }
        return;
      }
      // Update state.
      bool progressed = next_seq_ > last_tick_seq_;
      last_tick_seq_ = next_seq_;
      if (progressed) {
        BROKER_DEBUG("made progress since last tick");
        if (idle_ticks_ > 0)
          idle_ticks_ = 0;
        if (heartbeat_interval_ > 0 && num_ticks() % heartbeat_interval_ == 0)
          send_ack();
        return;
      }
      ++idle_ticks_;
      BROKER_DEBUG("made no progress for" << idle_ticks_ << "ticks");
      if (next_seq_ < last_seq_ && idle_ticks_ >= nack_timeout_) {
        idle_ticks_ = 0;
        auto first = next_seq_;
        auto last = last_seq_;
        std::vector<sequence_number_type> seqs;
        seqs.reserve(last - first);
        auto generate = [&, i{first}](sequence_number_type found) mutable {
          for (; i < found; ++i)
            seqs.emplace_back(i);
          ++i;
        };
        for (const auto& x : buf_)
          generate(x.seq);
        generate(last);
        backend_->send(this, nack{std::move(seqs)});
        return;
      }
      if (heartbeat_interval_ > 0 && num_ticks() % heartbeat_interval_ == 0)
        send_ack();
    }

    // -- properties -----------------------------------------------------------

    auto& backend() noexcept {
      return *backend_;
    }

    const auto& backend() const noexcept {
      return *backend_;
    }

    metrics_t& metrics() noexcept {
      return metrics_;
    }

    const metrics_t& metrics() const noexcept {
      return metrics_;
    }

    const auto& producer() const {
      return producer_;
    }

    void producer(Handle hdl) {
      producer_ = std::move(hdl);
    }

    const auto& buf() const noexcept {
      return buf_;
    }

    auto num_ticks() const noexcept {
      // Lamport timestamps start at 1.
      return tick_.value - 1;
    }

    auto idle_ticks() const noexcept {
      return idle_ticks_;
    }

    auto heartbeat_interval() const noexcept {
      return heartbeat_interval_;
    }

    auto heartbeat_interval(tick_interval_type value) noexcept {
      heartbeat_interval_ = value;
    }

    auto connection_timeout() const noexcept {
      return uint64_t{heartbeat_interval_} * connection_timeout_factor_;
    }

    auto connection_timeout_factor() const noexcept {
      return connection_timeout_factor_;
    }

    void connection_timeout_factor(tick_interval_type value) noexcept {
      connection_timeout_factor_ = value;
    }

    auto nack_timeout() const noexcept {
      return nack_timeout_;
    }

    void nack_timeout(tick_interval_type value) noexcept {
      nack_timeout_ = value;
    }

    auto next_seq() const noexcept {
      return next_seq_;
    }

    auto last_seq() const noexcept {
      return last_seq_;
    }

    bool initialized() const noexcept {
      return next_seq_ != 0;
    }

    bool idle() const noexcept {
      return initialized() && buf_.empty() && next_seq_ == last_seq_;
    }

    void reset() {
      if (initialized()) {
        metrics_.dec_input_channels();
      }
      producer_ = Handle{};
      next_seq_ = 0;
      last_seq_ = 0;
      buf_.clear();
      tick_ = lamport_timestamp{};
      last_tick_seq_ = 0;
      idle_ticks_ = 0;
      heartbeat_interval_ = 0;
      nack_timeout_ = 5;
    }

  private:
    // -- helper functions -----------------------------------------------------

    // Bumps the sequence number for the next expected event.
    void bump_seq() {
      if (++next_seq_ > last_seq_)
        last_seq_ = next_seq_;
    }

    // Consumes all events from buf_ until either hitting the end or hitting a
    // gap (i.e. events that are neither available yet nor known missing).
    void try_consume_buffer() {
      auto i = buf_.begin();
      for (; i != buf_.end() && i->seq == next_seq_; ++i) {
        if (i->content) {
          backend_->consume(this, *i->content);
        } else {
          if (auto err = backend_->consume_nil(this)) {
            buf_.erase(buf_.begin(), i);
            backend_->close(this, std::move(err));
            reset();
            return;
          }
        }
        bump_seq();
      }
      if (auto n = std::distance(buf_.begin(), i); n > 0) {
        buf_.erase(buf_.begin(), i);
        metrics_.dec_out_of_order_updates(n);
      }
    }

    void send_ack() {
      backend_->send(this, cumulative_ack{next_seq_ > 0 ? next_seq_ - 1 : 0});
    }

    // -- member variables -----------------------------------------------------

    /// Handles incoming events.
    Backend* backend_;

    /// Caches pointers to the metric instances.
    metrics_t metrics_;

    /// Stores the handle of the producer.
    Handle producer_;

    /// Monotonically increasing counter (starting at 1) to establish ordering
    /// of messages on this channel.
    sequence_number_type next_seq_ = 0;

    /// The currently known end of the event stream.
    sequence_number_type last_seq_ = 0;

    /// Stores outgoing events with their sequence number.
    buf_type buf_;

    /// Monotonically increasing counter to keep track of time.
    lamport_timestamp tick_;

    /// Stores the value of `next_seq_` at our last tick.
    sequence_number_type last_tick_seq_ = 0;

    /// Number of ticks without progress.
    tick_interval_type idle_ticks_ = 0;

    /// Frequency of ACK messages (configured by the master).
    tick_interval_type heartbeat_interval_ = 0;

    /// Number of ticks without progress before sending a NACK.
    tick_interval_type nack_timeout_ = 5;

    /// Factor for computing the timeout for producers, i.e., after how many
    /// heartbeats of not receiving any message do we assume the producer no
    /// longer exists.
    tick_interval_type connection_timeout_factor_ = 4;
  };
};

} // namespace broker::internal
