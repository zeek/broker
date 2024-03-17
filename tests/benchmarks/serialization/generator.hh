#include <broker/data.hh>
#include <broker/endpoint_id.hh>
#include <broker/time.hh>

#include <caf/uuid.hpp>

#include <random>
#include <string>

class generator {
public:
  static constexpr std::string_view charset = "0123456789"
                                              "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                                              "abcdefghijklmnopqrstuvwxyz";

  generator()
    : rng_(0xB7E57), byte_dis_(0, 255), char_dis_(0, charset.size() - 1) {
    // nop
  }

  broker::endpoint_id next_endpoint_id();

  std::string next_string(size_t length);

  broker::count next_count() {
    return count_dis_(rng_);
  }

  broker::integer next_integer() {
    return integer_dis_(rng_);
  }

  broker::timestamp next_timestamp() {
    ts_ += std::chrono::seconds(byte_dis_(rng_));
    return ts_;
  }

  broker::data next_data(int event_type);

private:
  std::minstd_rand rng_;
  std::uniform_int_distribution<> byte_dis_;
  std::uniform_int_distribution<size_t> char_dis_;
  std::uniform_int_distribution<broker::count> count_dis_;
  std::uniform_int_distribution<broker::integer> integer_dis_;
  broker::timestamp ts_;
};
