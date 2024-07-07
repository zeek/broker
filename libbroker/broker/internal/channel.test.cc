#include "broker/internal/channel.hh"

#include "broker/broker-test.test.hh"

#include <cmath>
#include <random>
#include <string>
#include <vector>

using namespace broker;

using namespace std::literals;

namespace {

// -- local types --------------------------------------------------------------

using channel_type = internal::channel<std::string, std::string>;

struct consumer_backend;
struct fixture;

using consumer_type = channel_type::consumer<consumer_backend>;
using producer_type = channel_type::producer<fixture>;

// -- consumer boilerplate code ------------------------------------------------

struct consumer_backend {
  std::string id;
  std::string input;
  std::string output;
  fixture* fix = nullptr;
  bool closed = false;
  bool fail_on_nil = false;

  consumer_backend() = default;

  explicit consumer_backend(std::string id) : id(std::move(id)) {
    // nop
  }

  void attach(fixture* fix) {
    this->fix = fix;
  }

  void consume(consumer_type*, std::string x) {
    input += x;
  }

  error consume_nil(consumer_type*) {
    input += '?';
    if (fail_on_nil)
      return make_error(ec::unspecified, "I really wanted that data! 😭");
    else
      return {};
  }

  template <class T>
  void send(consumer_type*, const T& x);

  void close(consumer_type*, error) {
    closed = true;
  }
};

struct consumer_state {
  consumer_backend backend;
  consumer_type consumer;
  consumer_state() : consumer(&backend) {
    // nop
  }
};

// -- fixture / producer boilerplate code --------------------------------------

struct fixture {
  struct outgoing_message {
    std::string src;

    std::string dst;

    std::variant<channel_type::producer_message, channel_type::consumer_message>
      content;

    template <class T>
    outgoing_message(std::string src, std::string dst, T&& xs)
      : src(std::move(src)), dst(std::move(dst)), content(std::forward<T>(xs)) {
      // nop
    }
  };

  std::string producer_log;

  producer_type producer;

  std::map<std::string, std::shared_ptr<consumer_state>> consumers;

  std::vector<outgoing_message> outgoing_messages;

  std::minstd_rand rng;

  fixture() : producer(this), rng(0xC00L) {
    // nop
  }

  void setup_consumers(std::initializer_list<std::string> consumer_names) {
    for (const auto& name : consumer_names) {
      auto new_consumer = std::make_shared<consumer_state>();
      new_consumer->backend.id = name;
      new_consumer->backend.attach(this);
      consumers.emplace(name, new_consumer);
      producer.add(name);
    }
    MESSAGE("setup: " << consumers);
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
      outgoing_messages.emplace_back("producer"s, i->first,
                                     channel_type::producer_message{x});
  }

  template <class T>
  void broadcast(producer_type*, const T& x) {
    producer_log += '\n';
    producer_log += render(producer.paths());
    producer_log += " <- ";
    producer_log += caf::deep_to_string(x);
    for (auto& kvp : consumers)
      outgoing_messages.emplace_back("producer"s, kvp.first,
                                     channel_type::producer_message{x});
  }

  void drop(producer_type*, const std::string& hdl, ec) {
    consumers.erase(hdl);
  }

  void handshake_completed(producer_type*, const std::string&) {
    // nop
  }

  // Uses a simulated transport channel that's beyond terrible. Randomly
  // reorders all messages and loses messages according to `loss_rate`.
  void ship(double loss_rate = 0) {
    assert(loss_rate < 1);
    if (outgoing_messages.empty())
      return;
    std::shuffle(outgoing_messages.begin(), outgoing_messages.end(), rng);
    if (loss_rate > 0) {
      auto num_message = outgoing_messages.size();
      auto lost = static_cast<size_t>(ceil(num_message * loss_rate));
      assert(num_message >= lost);
      auto new_size = std::max(num_message - lost, size_t{1});
      auto i = outgoing_messages.begin() + new_size;
      outgoing_messages.erase(i, outgoing_messages.end());
    }
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

  void tick() {
    producer.tick();
    for (const auto& kvp : consumers)
      kvp.second->consumer.tick();
  }

  void run() {
    decltype(outgoing_messages) tmp;
    tmp.swap(outgoing_messages);
    for (auto& msg : tmp) {
      auto handle_consumer_msg = [this, &msg](auto& content) {
        using content_type = std::decay_t<decltype(content)>;
        if constexpr (std::is_same_v<content_type,
                                     channel_type::cumulative_ack>) {
          producer.handle_ack(msg.src, content.seq);
        } else {
          static_assert(std::is_same_v<content_type, channel_type::nack>);
          producer.handle_nack(msg.src, content.seqs);
        }
      };
      auto handle_producer_msg = [this, &msg](auto& content) {
        using content_type = std::decay_t<decltype(content)>;
        auto* cptr = consumers[msg.dst].get();
        if (cptr == nullptr) {
          throw std::runtime_error("consumer not found");
        }
        if constexpr (std::is_same_v<content_type, channel_type::handshake>) {
          cptr->consumer.handle_handshake(content.offset,
                                          content.heartbeat_interval);
        } else if constexpr (std::is_same_v<content_type,
                                            channel_type::heartbeat>) {
          cptr->consumer.handle_heartbeat(content.seq);
        } else if constexpr (std::is_same_v<content_type,
                                            channel_type::event>) {
          cptr->consumer.handle_event(content.seq, content.content);
        } else {
          static_assert(
            std::is_same_v<content_type, channel_type::retransmit_failed>);
          cptr->consumer.handle_retransmit_failed(content.seq);
        }
      };
      auto handle_msg = [&](auto& content) {
        using content_type = std::decay_t<decltype(content)>;
        if constexpr (std::is_same_v<content_type,
                                     channel_type::producer_message>) {
          std::visit(handle_producer_msg, content);
        } else {
          static_assert(
            std::is_same_v<content_type, channel_type::consumer_message>);
          std::visit(handle_consumer_msg, content);
        }
      };
      std::visit(handle_msg, msg.content);
    }
  }

  void ship_run_tick(double loss_rate = 0) {
    ship(loss_rate);
    run();
    tick();
  }

  consumer_type& get(const std::string& id) {
    auto i = consumers.find(id);
    if (i == consumers.end())
      FAIL("unable to retrieve state for consumer " << id);
    return i->second->consumer;
  }
};

template <class T>
void consumer_backend::send(consumer_type*, const T& x) {
  if (!output.empty()) {
    output += '\n';
  }
  output += caf::deep_to_string(x);
  if (fix)
    fix->outgoing_messages.emplace_back(id, "producer"s,
                                        channel_type::consumer_message{x});
}

struct consumer_visitor {
  consumer_type* ch;

  void operator()(channel_type::handshake& msg) {
    ch->handle_handshake(msg.offset, msg.heartbeat_interval);
  }

  void operator()(channel_type::heartbeat& msg) {
    ch->handle_heartbeat(msg.seq);
  }

  void operator()(channel_type::event& msg) {
    ch->handle_event(msg.seq, msg.content);
  }

  void operator()(channel_type::retransmit_failed& msg) {
    ch->handle_retransmit_failed(msg.seq);
  }
};

} // namespace

// -- ye olde tests ------------------------------------------------------------

FIXTURE_SCOPE(channel_tests, fixture)

TEST(adding consumers triggers handshakes) {
  producer.add("A");
  CHECK_EQUAL(producer.seq(), 1u);
  producer.produce("abc");
  CHECK_EQUAL(producer.seq(), 2u);
  producer.produce("def");
  CHECK_EQUAL(producer.seq(), 3u);
  producer.add("B");
  producer.produce("ghi");
  CHECK_EQUAL(producer.seq(), 4u);
  CHECK_EQUAL(producer.buf().size(), 3u);
  CHECK_EQUAL(producer_log, R"(
A <- handshake(1, 5)
[A] <- event(2, "abc")
[A] <- event(3, "def")
B <- handshake(3, 5)
[A, B] <- event(4, "ghi"))");
}

TEST(ACKs delete elements from the buffer) {
  producer.add("A");
  producer.add("B");
  producer.add("C");
  producer.produce("a");
  CHECK_EQUAL(producer.buf().back().seq, 2u);
  producer.produce("b");
  CHECK_EQUAL(producer.buf().back().seq, 3u);
  producer.produce("c");
  CHECK_EQUAL(producer.buf().back().seq, 4u);
  producer.produce("d");
  CHECK_EQUAL(producer.buf().back().seq, 5u);
  CHECK_EQUAL(producer.buf().size(), 4u);
  producer.handle_ack("A", 3);
  CHECK_EQUAL(producer.buf().size(), 4u);
  producer.handle_ack("B", 4);
  CHECK_EQUAL(producer.buf().size(), 4u);
  producer.handle_ack("C", 5);
  CHECK_EQUAL(producer.buf().size(), 2u);
  CHECK_EQUAL(producer.buf().front().seq, 4u);
  producer.handle_ack("A", 5);
  CHECK_EQUAL(producer.buf().size(), 1u);
  CHECK_EQUAL(producer.buf().front().seq, 5u);
  producer.handle_ack("B", 5);
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
  CHECK_EQUAL(producer_log, "\nA <- handshake(1, 5)");
  producer_log.clear();
  MESSAGE("sending NACK for sequence number N re-sends the event");
  producer.handle_nack("B", {2, 4});
  CHECK_EQUAL(producer_log, R"(
B <- event(2, "a")
B <- event(4, "c"))");
  producer_log.clear();
  MESSAGE("sending NACK for unknown sequence numbers sends errors");
  producer.handle_ack("A", 5);
  producer.handle_ack("B", 5);
  CHECK_EQUAL(producer.buf().size(), 0u);
  producer.handle_nack("B", {2, 4});
  CHECK_EQUAL(producer_log, R"(
B <- retransmit_failed(2)
B <- retransmit_failed(4))");
}

TEST(consumers process events in order) {
  consumer_backend cb{"A"};
  consumer_type consumer{&cb};
  consumer.handle_handshake(0, 3);
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
  consumer.handle_handshake(0, 3);
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
  consumer.handle_handshake(2, 3);
  CHECK_EQUAL(consumer.buf().size(), 0u);
  CHECK_EQUAL(cb.input, "abc");
}

TEST(consumers send cumulative ACK messages) {
  consumer_backend cb{"A"};
  consumer_type consumer{&cb};
  consumer.handle_handshake(1, 1);
  cb.output.clear();
  MESSAGE("each tick triggers an ACK when setting heartbeat interval to 1");
  consumer.tick();
  CHECK_EQUAL(cb.output, "cumulative_ack(1)");
  consumer.tick();
  CHECK_EQUAL(cb.output, "cumulative_ack(1)\ncumulative_ack(1)");
  cb.output.clear();
  MESSAGE("after some events, the ACK contains the last received seq ID");
  consumer.handle_event(2, "a");
  consumer.handle_event(3, "b");
  consumer.tick();
  CHECK_EQUAL(cb.input, "ab");
  CHECK_EQUAL(cb.output, "cumulative_ack(3)");
}

TEST(consumers send NACK messages when receiving incomplete data) {
  consumer_backend cb{"A"};
  consumer_type consumer{&cb};
  consumer.nack_timeout(3);
  CHECK_EQUAL(consumer.num_ticks(), 0u);
  MESSAGE("the consumer sends a NACK after making no progress for two ticks");
  consumer.handle_handshake(1, 5);
  CHECK_EQUAL(cb.output, "cumulative_ack(1)");
  cb.output.clear();
  consumer.tick();
  CHECK_EQUAL(consumer.num_ticks(), 1u);
  CHECK_EQUAL(consumer.idle_ticks(), 0u);
  consumer.handle_event(5, "d");
  consumer.handle_event(3, "b");
  consumer.handle_event(8, "g");
  consumer.tick();
  CHECK_EQUAL(consumer.num_ticks(), 2u);
  CHECK_EQUAL(cb.input, "");
  CHECK_EQUAL(consumer.idle_ticks(), 1u);
  CHECK_EQUAL(cb.output, "");
  consumer.tick();
  CHECK_EQUAL(consumer.num_ticks(), 3u);
  CHECK_EQUAL(cb.input, "");
  CHECK_EQUAL(consumer.idle_ticks(), 2u);
  CHECK_EQUAL(cb.output, "");
  consumer.tick();
  CHECK_EQUAL(consumer.num_ticks(), 4u);
  CHECK_EQUAL(cb.input, "");
  CHECK_EQUAL(consumer.idle_ticks(), 0u);
  CHECK_EQUAL(cb.output, "nack([2, 4, 6, 7])");
  MESSAGE("the consumer sends an ack every five ticks, even without progress");
  cb.output.clear();
  consumer.tick();
  CHECK_EQUAL(consumer.num_ticks(), 5u);
  CHECK_EQUAL(cb.input, "");
  CHECK_EQUAL(consumer.idle_ticks(), 1u);
  CHECK_EQUAL(cb.output, "cumulative_ack(1)");
}

TEST(producers become idle after all consumers ACKed all messages) {
  setup_consumers({"A", "B", "C", "D"});
  producer.produce("a");
  producer.produce("b");
  producer.produce("c");
  producer.produce("d");
  ship_run_tick();
  producer.produce("e");
  producer.produce("f");
  producer.produce("g");
  producer.produce("h");
  ship_run_tick();
  producer.produce("i");
  producer.produce("j");
  producer.produce("k");
  producer.produce("l");
  ship();
  run();
  while (!producer.idle()) {
    tick();
    ship();
    run();
  }
  CHECK_EQUAL(producer.buf().size(), 0u);
  CHECK_EQUAL(get("A").backend().input, "abcdefghijkl");
  CHECK_EQUAL(get("B").backend().input, "abcdefghijkl");
  CHECK_EQUAL(get("C").backend().input, "abcdefghijkl");
  CHECK_EQUAL(get("D").backend().input, "abcdefghijkl");
}

TEST(messages arrive eventually - even with 33 percent loss rate) {
  producer.connection_timeout_factor(12);
  // Essentially the same test as above, but with a loss rate of 33%.
  setup_consumers({"A", "B", "C", "D"});
  CHECK_EQUAL(get("A").backend().input, "");
  CHECK_EQUAL(get("B").backend().input, "");
  CHECK_EQUAL(get("C").backend().input, "");
  CHECK_EQUAL(get("D").backend().input, "");
  producer.produce("a");
  producer.produce("b");
  producer.produce("c");
  producer.produce("d");
  ship_run_tick(0.33);
  producer.produce("e");
  producer.produce("f");
  producer.produce("g");
  producer.produce("h");
  ship_run_tick(0.33);
  producer.produce("i");
  producer.produce("j");
  producer.produce("k");
  producer.produce("l");
  ship(0.33);
  run();
  for (size_t round = 1; !producer.idle(); ++round) {
    if (round == 100)
      FAIL("system didn't reach a stable state after 100 rounds");
    tick();
    ship(0.33);
    run();
  }
  CHECK_EQUAL(producer.buf().size(), 0u);
  CHECK_EQUAL(get("A").backend().input, "abcdefghijkl");
  CHECK_EQUAL(get("B").backend().input, "abcdefghijkl");
  CHECK_EQUAL(get("C").backend().input, "abcdefghijkl");
  CHECK_EQUAL(get("D").backend().input, "abcdefghijkl");
}

TEST(messages arrive eventually - even with 66 percent loss rate) {
  producer.connection_timeout_factor(24);
  // Essentially the same test again, but with a loss rate of 66%.
  setup_consumers({"A", "B", "C", "D"});
  producer.produce("a");
  producer.produce("b");
  producer.produce("c");
  producer.produce("d");
  ship_run_tick(0.66);
  producer.produce("e");
  producer.produce("f");
  producer.produce("g");
  producer.produce("h");
  ship_run_tick(0.66);
  producer.produce("i");
  producer.produce("j");
  producer.produce("k");
  producer.produce("l");
  ship(0.66);
  run();
  for (size_t round = 1; !producer.idle(); ++round) {
    if (round == 500)
      FAIL("system didn't reach a stable state after 200 rounds");
    tick();
    ship(0.66);
    run();
  }
  CHECK_EQUAL(producer.buf().size(), 0u);
  CHECK_EQUAL(get("A").backend().input, "abcdefghijkl");
  CHECK_EQUAL(get("B").backend().input, "abcdefghijkl");
  CHECK_EQUAL(get("C").backend().input, "abcdefghijkl");
  CHECK_EQUAL(get("D").backend().input, "abcdefghijkl");
}

FIXTURE_SCOPE_END()
