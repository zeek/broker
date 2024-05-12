#include "generator.hh"

using namespace std::literals;

broker::endpoint_id generator::next_endpoint_id() {
  broker::endpoint_id::array_type bytes;
  for (auto& x : bytes)
    x = static_cast<std::byte>(byte_dis_(rng_));
  return broker::endpoint_id{bytes};
}

std::string generator::next_string(size_t length) {
  std::string result;
  result.resize(length);
  for (auto& c : result)
    c = charset[char_dis_(rng_)];
  return result;
}

broker::data generator::next_data(int event_type) {
  using std::chrono::duration_cast;
  broker::vector result;
  switch (event_type) {
    case 1: {
      result.reserve(2);
      result.emplace_back(42);
      result.emplace_back("test"s);
      break;
    }
    case 2: {
      auto tcp = broker::port::protocol::tcp;
      broker::address a1;
      broker::address a2;
      broker::convert("1.2.3.4", a1);
      broker::convert("3.4.5.6", a2);
      result.emplace_back(next_timestamp());
      result.emplace_back(next_string(10));
      result.emplace_back(
        broker::vector{a1, broker::port(4567, tcp), a2, broker::port(80, tcp)});
      result.emplace_back(broker::enum_value("tcp"));
      result.emplace_back(next_string(10));
      result.emplace_back(duration_cast<broker::timespan>(3140ms));
      result.emplace_back(next_count());
      result.emplace_back(next_count());
      result.emplace_back(next_string(5));
      result.emplace_back(true);
      result.emplace_back(false);
      result.emplace_back(next_count());
      result.emplace_back(next_string(10));
      result.emplace_back(next_count());
      result.emplace_back(next_count());
      result.emplace_back(next_count());
      result.emplace_back(next_count());
      result.emplace_back(broker::set({next_string(10), next_string(10)}));
      break;
    }
    case 3: {
      broker::table m;
      for (int i = 0; i < 100; ++i) {
        broker::set s;
        for (int j = 0; j < 10; ++j)
          s.insert(next_string(5));
        m[next_string(15)] = std::move(s);
      }
      result.emplace_back(next_timestamp());
      result.emplace_back(std::move(m));
      break;
    }
    default: {
      fprintf(stderr, "event type must be 1, 2, or 3; got %d\n", event_type);
      throw std::logic_error("invalid event type");
    }
  }
  return broker::data{std::move(result)};
}
