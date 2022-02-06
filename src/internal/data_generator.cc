#include "broker/internal/data_generator.hh"

#include <string>
#include <vector>

#include <caf/binary_deserializer.hpp>
#include <caf/binary_serializer.hpp>
#include <caf/sec.hpp>

#include "broker/internal/logger.hh"
#include "broker/internal/meta_data_writer.hh"
#include "broker/internal/read_value.hh"
#include "broker/internal/type_id.hh"

using std::string;

#define GENERATE_CASE(type_name)                                               \
  case data::type::type_name: {                                                \
    type_name tmp;                                                             \
    if (auto err = generate(tmp))                                              \
      return err;                                                              \
    x = std::move(tmp);                                                        \
    break;                                                                     \
  }

#define GENERATE(var_name)                                                     \
  if (auto err = generate(var_name))                                           \
  return err

namespace broker::internal {

namespace {

template <class T>
void recreate(data_generator& self, T& xs) {
  caf::binary_serializer::container_type buf;
  caf::binary_serializer sink{nullptr, buf};
  meta_data_writer writer{sink};
  if (auto err = writer(xs)) {
    BROKER_ERROR("unable to generate meta data: " << err);
    return;
  }
  caf::binary_deserializer source{nullptr, buf};
  unsigned seed = 0;
  self.shuffle(seed);
  data_generator g{source, seed};
  T tmp;
  if (auto err = g.generate(tmp)) {
    BROKER_ERROR("unable to generate data: " << err);
    return;
  }
  xs = std::move(tmp);
}

} // namespace

data_generator::data_generator(caf::binary_deserializer& meta_data_source,
                               unsigned seed)
  : source_(meta_data_source),
    engine_(seed),
    char_generator_('!', '}'),
    byte_generator_(0, 255) {
  // nop
}

caf::error data_generator::operator()(data& x) {
  return generate(x);
}

caf::error data_generator::operator()(internal_command& x) {
  return generate(x);
}

caf::error data_generator::generate(data& x) {
  auto tag = data::type{};
  BROKER_TRY(read_value(source_, tag));
  return generate(tag, x);
}

caf::error data_generator::generate(data::type tag, data& x) {
  switch (tag) {
    GENERATE_CASE(none)
    GENERATE_CASE(boolean)
    GENERATE_CASE(count)
    GENERATE_CASE(integer)
    GENERATE_CASE(real)
    GENERATE_CASE(string)
    GENERATE_CASE(address)
    GENERATE_CASE(subnet)
    GENERATE_CASE(port)
    GENERATE_CASE(timestamp)
    GENERATE_CASE(timespan)
    GENERATE_CASE(enum_value)
    GENERATE_CASE(set)
    GENERATE_CASE(table)
    GENERATE_CASE(vector)
    default:
      return caf::sec::invalid_argument;
  }
  return caf::none;
}

caf::error data_generator::generate(internal_command& x) {
  internal_command::type tag{};
  BROKER_TRY(read_value(source_, tag));
  return generate(tag, x);
}

caf::error data_generator::generate(internal_command::type tag,
                                    internal_command& x) {
  using tag_type = internal_command::type;
  switch (tag) {
    case tag_type::put_command: {
      data key;
      data val;
      GENERATE(key);
      GENERATE(val);
      x.content = put_command{std::move(key), std::move(val), std::nullopt};
      break;
    }
    case tag_type::put_unique_command: {
      data key;
      data val;
      GENERATE(key);
      GENERATE(val);
      x.content = put_unique_command{std::move(key), std::move(val),
                                     std::nullopt, entity_id::nil(), 0};
      break;
    }
    case tag_type::erase_command: {
      data key;
      GENERATE(key);
      x.content = erase_command{std::move(key)};
      break;
    }
    case tag_type::expire_command: {
      data key;
      GENERATE(key);
      x.content = expire_command{std::move(key)};
      break;
    }
    case tag_type::add_command: {
      data key;
      data val;
      data::type init_type{};
      GENERATE(key);
      GENERATE(val);
      BROKER_TRY(read_value(source_, init_type));
      x.content = add_command{std::move(key), std::move(val), init_type,
                              std::nullopt};
      break;
    }
    case tag_type::subtract_command: {
      data key;
      data val;
      GENERATE(key);
      GENERATE(val);
      x.content = subtract_command{std::move(key), std::move(val)};
      break;
    }
    case tag_type::clear_command: {
      x.content = clear_command{};
      break;
    }
    case tag_type::attach_clone_command: {
      x.content = attach_clone_command{};
      break;
    }
    case tag_type::attach_writer_command: {
      sequence_number_type offset;
      tick_interval_type heartbeat_interval;
      GENERATE(offset);
      GENERATE(heartbeat_interval);
      x.content = attach_writer_command{offset, heartbeat_interval};
      break;
    }
    case tag_type::keepalive_command: {
      sequence_number_type seq;
      GENERATE(seq);
      x.content = keepalive_command{seq};
      break;
    }
    case tag_type::cumulative_ack_command: {
      sequence_number_type seq;
      GENERATE(seq);
      x.content = cumulative_ack_command{seq};
      break;
    }
    case tag_type::nack_command: {
      std::vector<sequence_number_type> seqs;
      GENERATE(seqs);
      x.content = nack_command{std::move(seqs)};
      break;
    }
    case tag_type::ack_clone_command: {
      sequence_number_type seq;
      tick_interval_type heartbeat_interval;
      snapshot state;
      GENERATE(seq);
      GENERATE(heartbeat_interval);
      GENERATE(state);
      x.content = ack_clone_command{seq, heartbeat_interval, std::move(state)};
      break;
    }
    case tag_type::retransmit_failed_command: {
      sequence_number_type offset;
      GENERATE(offset);
      x.content = retransmit_failed_command{offset};
      break;
    }
    default:
      return ec::invalid_tag;
  }
  return caf::none;
}

caf::error data_generator::generate(vector& xs) {
  uint32_t size = 0;
  BROKER_TRY(read_value(source_, size));
  for (size_t i = 0; i < size; ++i) {
    data value;
    GENERATE(value);
    xs.emplace_back(std::move(value));
  }
  return caf::none;
}

caf::error data_generator::generate(set& xs) {
  uint32_t size = 0;
  BROKER_TRY(read_value(source_, size));
  data value;
  for (size_t i = 0; i < size; ++i) {
    GENERATE(value);
    while (!xs.emplace(value).second)
      shuffle(value);
  }
  return caf::none;
}

caf::error data_generator::generate(table& xs) {
  uint32_t size = 0;
  BROKER_TRY(read_value(source_, size));
  data key;
  data value;
  for (size_t i = 0; i < size; ++i) {
    GENERATE(key);
    GENERATE(value);
    while (!xs.emplace(key, value).second)
      shuffle(key);
  }
  return caf::none;
}

caf::error data_generator::generate(std::unordered_map<data, data>& xs) {
  uint32_t size = 0;
  BROKER_TRY(read_value(source_, size));
  data key;
  data value;
  for (size_t i = 0; i < size; ++i) {
    GENERATE(key);
    GENERATE(value);
    while (!xs.emplace(key, value).second)
      shuffle(key);
  }
  return caf::none;
}

caf::error data_generator::generate(std::string& x) {
  uint32_t string_size = 0;
  BROKER_TRY(read_value(source_, string_size));
  x.insert(x.end(), string_size, 'x');
  return caf::none;
}

caf::error data_generator::generate(enum_value& x) {
  std::string name;
  GENERATE(name);
  x.name = std::move(name);
  return caf::none;
}

void data_generator::shuffle(none&) {
  // nop
}

void data_generator::shuffle(boolean& x) {
  x = engine_() % 2 != 0;
}

void data_generator::shuffle(std::string& x) {
  for (size_t i = 0; i < x.size(); ++i)
    x[i] = next_char();
}

void data_generator::shuffle(enum_value& x) {
  shuffle(x.name);
}

void data_generator::shuffle(port& x) {
  uint16_t num = uint16_t{next_byte()} << 8;
  num |= next_byte();
  auto p = static_cast<port::protocol>(next_byte() % 4);
  x = port{num, p};
}

void data_generator::shuffle(address& x) {
  for (auto& byte : x.bytes())
    byte = next_byte();
}

void data_generator::shuffle(subnet& x) {
  address addr;
  shuffle(addr);
  x = subnet{addr, next_byte()};
}

void data_generator::shuffle(timespan& x) {
  x = timespan{engine_()};
}

void data_generator::shuffle(timestamp& x) {
  x = timestamp{timespan{engine_()}};
}

void data_generator::shuffle(data& x) {
  mixer f{*this};
  visit(f, x);
}

void data_generator::shuffle(set& xs) {
  // We can't reasonably shuffle a set, so just create a new one.
  recreate(*this, xs);
}

void data_generator::shuffle(table& xs) {
  // Just like sets, tables don't allow us to modifies the keys.
  recreate(*this, xs);
}

char data_generator::next_char() {
  // Unfortunately, the standard does not permit std::uniform_int_distribution
  // to produce 8-bit integer types. We need to work around this issue by
  // producing 16-bit integers and then keep only the lower bits. We initialize
  // char_generator to produce only valid characters in the constructor.
  return static_cast<char>(char_generator_(engine_));
}

uint8_t data_generator::next_byte() {
  // Same as above: work around weird restriction.
  return static_cast<uint8_t>(byte_generator_(engine_));
}

} // namespace broker::internal
