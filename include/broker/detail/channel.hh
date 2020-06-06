#pragma once

#include <algorithm>
#include <cstdint>
#include <deque>

#include <caf/actor.hpp>
#include <caf/meta/type_name.hpp>
#include <caf/send.hpp>

#include "broker/error.hh"

namespace broker::detail {

/// A message-driven channel for ensuring reliable and ordererd transport over
/// an unreliable and unordered communication layer. A channel belongs to a
/// single producer with any number of consumers.
template <class Handle, class Payload>
class channel {
public:
  /// Integer type for the monotonically increasing counters large enough to
  /// neglect wrap aroundss. At 1000 messages per second, a sequence number of
  /// this type overflows after 580 *million* years.
  using sequence_number_type = uint64_t;

  /// Notifies the producer that a consumer received all events up to a certain
  /// sequence number (including that number).
  struct cumulative_ack {
    sequence_number_type seq;

    template <class Inspector>
    friend typename Inspector::result_type inspect(Inspector& f,
                                                   cumulative_ack& x) {
      return f(caf::meta::type_name("cumulative_ack"), x.seq);
    }
  };

  /// Notifies the producer that a consumer failed to received some events.
  /// Sending a NACK for the sequence number 0 causes the publisher to re-send
  /// the handshake.
  struct nack {
    std::vector<sequence_number_type> seqs;

    template <class Inspector>
    friend typename Inspector::result_type inspect(Inspector& f, nack& x) {
      return f(caf::meta::type_name("nack"), x.seqs);
    }
  };

  /// Notifies a consumer which is the first sequence number after it started
  /// listening to the producer.
  struct handshake {
    /// The first sequence number a consumer should process and acknowledge.
    sequence_number_type first_seq;

    template <class Inspector>
    friend typename Inspector::result_type inspect(Inspector& f, handshake& x) {
      return f(caf::meta::type_name("handshake"), x.first_seq);
    }
  };

  /// Transmits ordered data to a consumer.
  struct event {
    sequence_number_type seq;
    Payload content;

    template <class Inspector>
    friend typename Inspector::result_type inspect(Inspector& f, event& x) {
      return f(caf::meta::type_name("event"), x.seq, x.content);
    }
  };

  /// Notifies a consumer that the producer can no longer retransmit an event.
  struct retransmit_failed {
    sequence_number_type seq;

    template <class Inspector>
    friend typename Inspector::result_type inspect(Inspector& f,
                                                   retransmit_failed& x) {
      return f(caf::meta::type_name("retransmit_failed"), x.seq);
    }
  };

  template <class Backend>
  class producer {
  public:
    using message = caf::variant<handshake, event>;

    /// Bundles consumer handle, offset and last acknowledged sequence number.
    struct path {
      Handle hdl;
      sequence_number_type offset;
      sequence_number_type acked;
    };

    using buf_type = std::deque<event>;

    using path_list = std::vector<path>;

    explicit producer(Backend* backend) : backend_(backend) {
      // nop
    }

    void produce(Payload content) {
      ++seq_;
      buf_.emplace_back(event{seq_, std::move(content)});
      backend_->send(paths_, buf_.back());
    }

    bool idle() const noexcept {
      auto at_head = [seq{seq_}](const path& x) { return x.acked == seq; };
      return std::all_of(paths_.begin(), paths_.end(), at_head);
    }

    error add(const Handle& hdl) {
      if (find_path(hdl) != paths_.end())
        return ec::consumer_exists;
      auto offset = seq_ + 1;
      paths_.emplace_back(path{hdl, offset, seq_});
      backend_->send(hdl, handshake{offset});
      return nil;
    }

    void handle_ack(const Handle& hdl, sequence_number_type seq) {
      sequence_number_type acked = seq;
      // Iterate all paths once, fetching minimum acknowledged sequence number
      // and updating the path belonging to `hdl` in one go.
      for (auto& x : paths_) {
        if (x.hdl == hdl)
          x.acked = seq;
        else
          acked = std::min(x.acked, acked);
      }
      // Drop events from the buffer if possible.
      auto not_acked = [acked](const event& x) { return x.seq > acked; };
      buf_.erase(buf_.begin(),
                 std::find_if(buf_.begin(), buf_.end(), not_acked));
    }

    void handle_nack(const Handle& hdl,
                     const std::vector<sequence_number_type>& seqs) {
      auto p = find_path(hdl);
      if (p == paths_.end())
        return;
      for (auto seq : seqs) {
        if (seq == 0)
          backend_->send(hdl, handshake{p->offset});
        else if (auto i = find_event(seq); i != buf_.end())
          backend_->send(hdl, *i);
        else
          backend_->send(hdl, retransmit_failed{seq});
      }
    }

    // -- properties -

    auto seq() const noexcept {
      return seq_;
    }

    const auto& buf() const noexcept {
      return buf_;
    }

    auto find_path(const Handle& hdl) const noexcept {
      auto has_hdl = [&hdl](const path& x) { return x.hdl == hdl; };
      return std::find_if(paths_.begin(), paths_.end(), has_hdl);
    }

    auto find_event(sequence_number_type seq) const noexcept {
      auto has_seq = [seq](const event& x) { return x.seq == seq; };
      return std::find_if(buf_.begin(), buf_.end(), has_seq);
    }

  private:
    /// Transmits messages to the consumers.
    Backend* backend_;

    /// Monotonically increasing counter (starting at 1) to establish ordering
    /// of messages on this channel.
    sequence_number_type seq_ = 0;

    /// Stores outgoing events with their sequence number.
    buf_type buf_;

    /// List of consumers with the last acknowledged sequence number.
    path_list paths_;
  };

  template <class Backend>
  class consumer {
  public:
    using buf_type = std::deque<event>;

    explicit consumer(Backend* backend) : backend_(backend) {
      // nop
    }

    void handle_handshake(sequence_number_type offset) {
      if (offset > next_seq_) {
        next_seq_ = offset + 1;
        try_consume_buffer();
      }
    }

    template <class T>
    void handle_event(sequence_number_type seq, T&& payload) {
      if (next_seq_ == seq) {
        backend_->consume(std::forward<T>(payload));
        ++next_seq_;
        try_consume_buffer();
      } else if (seq > next_seq_) {
        // Insert event into buf_ (ordered by the sequence number).
        auto pred = [seq](const event& x) { return x.seq >= seq; };
        auto i = std::find_if(buf_.begin(), buf_.end(), pred);
        if (i == buf_.end())
          buf_.emplace_back(event{seq, std::forward<T>(payload)});
        else if (i->seq != seq)
          buf_.emplace(i, event{seq, std::forward<T>(payload)});
      }
    }

    const buf_type& buf() const noexcept {
      return buf_;
    }

  private:
    void try_consume_buffer() {
      auto i = buf_.begin();
      for (; i != buf_.end() && i->seq == next_seq_; ++i) {
        backend_->consume(std::move(i->content));
        ++next_seq_;
      }
      buf_.erase(buf_.begin(), i);
    }

    /// Handles incoming events.
    Backend* backend_;

    /// Monotonically increasing counter (starting at 1) to establish ordering
    /// of messages on this channel.
    sequence_number_type next_seq_ = 1;

    /// Stores outgoing events with their sequence number.
    buf_type buf_;
  };
};

} // namespace broker::detail
