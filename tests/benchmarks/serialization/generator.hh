#include <broker/data.hh>
#include <broker/endpoint_id.hh>
#include <broker/time.hh>

#include <caf/uuid.hpp>

#include <random>
#include <string>

class generator {
public:
  generator();

  broker::endpoint_id next_endpoint_id();

  static auto make_endpoint_id() {
    generator g;
    return g.next_endpoint_id();
  }

  broker::count next_count();

  caf::uuid next_uuid();

  std::string next_string(size_t length);

  broker::timestamp next_timestamp();

  // Generates events for one of three possible types:
  // 1. Trivial data consisting of a number and a string.
  // 2. More complex data that resembles a line in a conn.log.
  // 3. Large tables of size 100 by 10, filled with random strings.
  broker::data next_data(size_t event_type);

  static auto make_data(size_t event_type) {
    generator g;
    return g.next_data(event_type);
  }

private:
  std::minstd_rand rng_;
  broker::timestamp ts_;
};
