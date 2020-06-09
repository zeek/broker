#define SUITE detail.channel

#include "broker/detail/channel.hh"

#include "test.hh"

#include <string>

using namespace broker;

namespace {

using channel_type = detail::channel<std::string, std::string>;

struct consumer_backend {
  std::string input;
  std::string output;

  void consume(const std::string& x) {
    input += x;
  }

  template <class T>
  void send(const T& x) {
    if (!output.empty())
      output += '\n';
    output += caf::deep_to_string(x);
  }
};

struct fixture : base_fixture {

  using consumer_type = channel_type::consumer<consumer_backend>;

  std::string producer_log;

  channel_type::producer<fixture> producer;

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

  template <class Destination, class T>
  void send(const Destination& dst, const T& x) {
    producer_log += '\n';
    if constexpr (std::is_same<Destination, std::string>::value)
      producer_log += dst;
    else
      producer_log += render(dst);
    producer_log += " <- ";
    producer_log += caf::deep_to_string(x);
  }

  std::map<std::string, consumer_type> consumers;
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
  consumer_backend cb;
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

TEST(consumers buffer events until receiving the handshake) {
  consumer_backend cb;
  consumer_type consumer{&cb};
  consumer.handle_event(3, "a");
  consumer.handle_event(4, "b");
  consumer.handle_event(5, "c");
  consumer.handle_handshake(2);
  CHECK_EQUAL(consumer.buf().size(), 0u);
  CHECK_EQUAL(cb.input, "abc");
}

TEST(consumers send cumulative ACK messages) {
  consumer_backend cb;
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
  consumer_backend cb;
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
