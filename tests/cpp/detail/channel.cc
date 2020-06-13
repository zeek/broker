#define SUITE detail.channel

#include "broker/detail/channel.hh"

#include "test.hh"

#include <string>

using namespace broker;

namespace {

// -- local types --------------------------------------------------------------

using channel_type = detail::channel<std::string, std::string>;

struct consumer_backend;
struct fixture;

using consumer_type = channel_type::consumer<consumer_backend>;
using producer_type = channel_type::producer<fixture>;

// -- consumer boilerplate code ------------------------------------------------

struct consumer_backend {
  std::string id;
  std::string input;
  std::string output;
  caf::event_based_actor* self = nullptr;
  caf::actor producer_hdl;
  bool closed = false;
  bool fail_on_nil = false;

  explicit consumer_backend(std::string id) : id(std::move(id)) {
    // nop
  }

  void attach(caf::event_based_actor* self, caf::actor producer_hdl) {
    this->self = self;
    this->producer_hdl = std::move(producer_hdl);
  }

  void consume(consumer_type*, std::string x) {
    input += x;
  }

  error consume_nil(consumer_type*) {
    input += '?';
    if (fail_on_nil)
      return make_error(ec::unspecified, "I really wanted that data! 😭");
    return nil;
  }

  template <class T>
  void send(consumer_type*, const T& x) {
    if (!output.empty())
      output += '\n';
    output += caf::deep_to_string(x);
    if (self)
      self->send(producer_hdl, consumer_msg{id, {x}});
  }

  void close(consumer_type*, error) {
    closed = true;
  }
};

struct consumer_visitor {
  channel_type::consumer<consumer_backend>* ch;

  void operator()(channel_type::handshake& msg) {
    ch->handle_handshake(msg.first_seq);
  }

  void operator()(channel_type::event& msg) {
    ch->handle_event(msg.seq, msg.content);
  }

  void operator()(channel_type::retransmit_failed& msg) {
    ch->handle_retransmit_failed(msg.seq);
  }
};

caf::behavior consumer_actor(caf::event_based_actor* self, consumer_type* ch,
                             caf::actor producer_hdl) {
  ch->backend().attach(self, producer_hdl);
  return {
    [ch](channel_type::producer_message& msg) {
    },
  };
}

// -- fixture / producer boilerplate code --------------------------------------

caf::behavior producer_actor(caf::event_based_actor* self,
                             channel_type::producer<fixture>* state);

struct fixture : base_fixture {

  using consumer_type = channel_type::consumer<consumer_backend>;

  std::string producer_log;

  producer_type producer;

  caf::actor producer_hdl;

  fixture() : producer(this) {
    // nop
  }

  template <class Paths>
  std::string render(const Paths& xs) {
    if (xs.empty())
      return "[]";
    std::string result = "[";
    auto i = xs.begin();
    result += i->hdl;
    for (++i; i != xs.end(); ++i) {
      result += ", ";
      result += i->hdl;
    }
    result += ']';
    return result;
  }

  template <class T>
  void send(producer_type*, const std::string& dst, const T& x) {
    producer_log += '\n';
    producer_log += dst;
    producer_log += " <- ";
    producer_log += caf::deep_to_string(x);
    if (auto i = consumers.find(dst); i != consumers.end())
      caf::send_as(producer_hdl, i->second, channel_type::producer_message{x});
  }

  template <class T>
  void broadcast(producer_type*, const T& x) {
    producer_log += '\n';
    producer_log += render(producer.paths());
    producer_log += " <- ";
    producer_log += caf::deep_to_string(x);
    for (auto& kvp : consumers)
      caf::send_as(producer_hdl, kvp.second, channel_type::producer_message{x});
  }

  std::string render_buffer(consumer_type& ref) {
    std::string result;
    for (auto& x : ref.buf())
      if (x.content)
        result += *x.content;
      else
        result += '?';
    return result;
  }

  std::map<std::string, caf::actor> consumers;
};

} // namespace

FIXTURE_SCOPE(channel_tests, fixture)

TEST(adding consumers triggers handshakes) {
  producer.add("A");
  CHECK_EQUAL(producer.seq(), 0u);
  producer.produce("abc");
  CHECK_EQUAL(producer.seq(), 1u);
  producer.produce("def");
  CHECK_EQUAL(producer.seq(), 2u);
  producer.add("B");
  producer.produce("ghi");
  CHECK_EQUAL(producer.seq(), 3u);
  CHECK_EQUAL(producer.buf().size(), 3u);
  CHECK_EQUAL(producer_log, R"(
A <- handshake(1)
[A] <- event(1, "abc")
[A] <- event(2, "def")
B <- handshake(3)
[A, B] <- event(3, "ghi"))");
}

TEST(ACKs delete elements from the buffer) {
  producer.add("A");
  producer.add("B");
  producer.add("C");
  producer.produce("a");
  CHECK_EQUAL(producer.buf().back().seq, 1u);
  producer.produce("b");
  CHECK_EQUAL(producer.buf().back().seq, 2u);
  producer.produce("c");
  CHECK_EQUAL(producer.buf().back().seq, 3u);
  producer.produce("d");
  CHECK_EQUAL(producer.buf().back().seq, 4u);
  CHECK_EQUAL(producer.buf().size(), 4u);
  producer.handle_ack("A", 2);
  CHECK_EQUAL(producer.buf().size(), 4u);
  producer.handle_ack("B", 3);
  CHECK_EQUAL(producer.buf().size(), 4u);
  producer.handle_ack("C", 4);
  CHECK_EQUAL(producer.buf().size(), 2u);
  CHECK_EQUAL(producer.buf().front().seq, 3u);
  producer.handle_ack("A", 4);
  CHECK_EQUAL(producer.buf().size(), 1u);
  CHECK_EQUAL(producer.buf().front().seq, 4u);
  producer.handle_ack("B", 4);
  CHECK_EQUAL(producer.buf().size(), 0u);
}

TEST(NACKs cause the producer to send messages again) {
  producer.add("A");
  producer.add("B");
  producer.produce("a");
  producer.produce("b");
  producer.produce("c");
  producer.produce("d");
  CHECK_EQUAL(producer.buf().size(), 4u);
  producer_log.clear();
  MESSAGE("sending NACK for 0 re-sends the handshake");
  producer.handle_nack("A", {0});
  CHECK_EQUAL(producer_log, "\nA <- handshake(1)");
  producer_log.clear();
  MESSAGE("sending NACK for sequence number N re-sends the event");
  producer.handle_nack("B", {1, 3});
  CHECK_EQUAL(producer_log, R"(
B <- event(1, "a")
B <- event(3, "c"))");
  producer_log.clear();
  MESSAGE("sending NACK for unknown sequence numbers sends errors");
  producer.handle_ack("A", 4);
  producer.handle_ack("B", 4);
  CHECK_EQUAL(producer.buf().size(), 0u);
  producer.handle_nack("B", {1, 3});
  CHECK_EQUAL(producer_log, R"(
B <- retransmit_failed(1)
B <- retransmit_failed(3))");
}

TEST(consumers process events in order) {
  consumer_backend cb{"A"};
  consumer_type consumer{&cb};
  consumer.handle_handshake(0);
  consumer.handle_event(4, "d");
  CHECK_EQUAL(consumer.buf().size(), 1u);
  consumer.handle_event(5, "e");
  CHECK_EQUAL(consumer.buf().size(), 2u);
  consumer.handle_event(5, "e");
  CHECK_EQUAL(consumer.buf().size(), 2u);
  consumer.handle_event(2, "b");
  CHECK_EQUAL(consumer.buf().size(), 3u);
  consumer.handle_event(3, "c");
  CHECK_EQUAL(consumer.buf().size(), 4u);
  consumer.handle_event(1, "a");
  CHECK_EQUAL(consumer.buf().size(), 0u);
  CHECK_EQUAL(cb.input, "abcde");
  consumer.handle_event(1, "a");
  CHECK_EQUAL(consumer.buf().size(), 0u);
  CHECK_EQUAL(cb.input, "abcde");
}

TEST(consumers process nil events if retransmits fail) {
  consumer_backend cb{"A"};
  consumer_type consumer{&cb};
  consumer.handle_handshake(0);
  consumer.handle_event(4, "d");
  consumer.handle_event(6, "f");
  CAF_MESSAGE("failed retransmits cause holes in the buffer");
  consumer.handle_retransmit_failed(5);
  CAF_CHECK_EQUAL(render_buffer(consumer), "d?f");
  CAF_MESSAGE("retransmit_failed has no effect on already received messages");
  consumer.handle_event(2, "b");
  consumer.handle_retransmit_failed(2);
  CAF_CHECK_EQUAL(render_buffer(consumer), "bd?f");
  CAF_MESSAGE("messages that arrive before processing lost messages count");
  consumer.handle_retransmit_failed(3);
  CAF_CHECK_EQUAL(render_buffer(consumer), "b?d?f");
  consumer.handle_event(3, "c");
  CAF_CHECK_EQUAL(render_buffer(consumer), "bcd?f");
  CAF_MESSAGE("the consumer calls consume and consume_nil as needed");
  consumer.handle_event(1, "a");
  CHECK_EQUAL(cb.input, "abcd?f");
  CHECK_EQUAL(cb.closed, false);
  CAF_MESSAGE("the consumer stops and closes if consume_nil returns an error");
  cb.fail_on_nil = true;
  consumer.handle_event(9, "i");
  consumer.handle_retransmit_failed(8);
  consumer.handle_event(7, "g");
  CHECK_EQUAL(cb.input, "abcd?fg?");
  CHECK_EQUAL(cb.closed, true);
}

TEST(consumers buffer events until receiving the handshake) {
  consumer_backend cb{"A"};
  consumer_type consumer{&cb};
  consumer.handle_event(3, "a");
  consumer.handle_event(4, "b");
  consumer.handle_event(5, "c");
  consumer.handle_handshake(2);
  CHECK_EQUAL(consumer.buf().size(), 0u);
  CHECK_EQUAL(cb.input, "abc");
}

TEST(consumers send cumulative ACK messages) {
  consumer_backend cb{"A"};
  consumer_type consumer{&cb};
  MESSAGE("each tick triggers an ACK");
  consumer.tick();
  CHECK_EQUAL(cb.output, "cumulative_ack(0)");
  consumer.handle_handshake(0);
  consumer.tick();
  CHECK_EQUAL(cb.output, "cumulative_ack(0)\ncumulative_ack(0)");
  cb.output.clear();
  MESSAGE("after some events, the ACK contains the last received seq ID");
  consumer.handle_event(1, "a");
  consumer.handle_event(2, "b");
  consumer.tick();
  CHECK_EQUAL(cb.input, "ab");
  CHECK_EQUAL(cb.output, "cumulative_ack(2)");
}

TEST(consumers send NACK messages when receiving incomplete data) {
  consumer_backend cb{"A"};
  consumer_type consumer{&cb};
  consumer.ack_interval(5);
  consumer.nack_timeout(3);
  MESSAGE("the consumer sends a NACK after making no progress for two ticks");
  consumer.handle_handshake(0);
  consumer.tick();
  CHECK_EQUAL(consumer.idle_ticks(), 0u);
  consumer.handle_event(4, "d");
  consumer.handle_event(2, "b");
  consumer.handle_event(7, "g");
  consumer.tick();
  CHECK_EQUAL(cb.input, "");
  CHECK_EQUAL(consumer.idle_ticks(), 1u);
  CHECK_EQUAL(cb.output, "");
  consumer.tick();
  CHECK_EQUAL(cb.input, "");
  CHECK_EQUAL(consumer.idle_ticks(), 2u);
  CHECK_EQUAL(cb.output, "");
  consumer.tick();
  CHECK_EQUAL(cb.input, "");
  CHECK_EQUAL(consumer.idle_ticks(), 0u);
  CHECK_EQUAL(cb.output, "nack([1, 3, 5, 6])");
  MESSAGE("the consumer sends an ack every five ticks, even without progress");
  cb.output.clear();
  consumer.tick();
  CHECK_EQUAL(cb.input, "");
  CHECK_EQUAL(consumer.idle_ticks(), 1u);
  CHECK_EQUAL(cb.output, "cumulative_ack(0)");
}

FIXTURE_SCOPE_END()
