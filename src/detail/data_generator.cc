#include "broker/detail/data_generator.hh"

#include <string>
#include <vector>

#include <caf/binary_deserializer.hpp>
#include <caf/binary_serializer.hpp>
#include <caf/sec.hpp>

#include "broker/detail/meta_data_writer.hh"
#include "broker/logger.hh"

using std::string;

#define GENERATE_CASE(type_name)                                               \
  case data::type::type_name: {                                                \
    type_name tmp;                                                             \
    if (auto err = generate(tmp))                                              \
      return err;                                                              \
    x = std::move(tmp);                                                        \
    break;                                                                     \
  }

#define READ(var_name)                                                         \
  if (auto err = source_(var_name))                                            \
  return err

#define GENERATE(var_name)                                                     \
  if (auto err = generate(var_name))                                           \
  return err

namespace broker {
namespace detail {

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
}

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
  data::type tag{};
  READ(tag);
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
  READ(tag);
  return generate(tag, x);
}

caf::error data_generator::generate(internal_command::type tag,
                                    internal_command& x) {
  using tag_type = internal_command::type;
  switch (tag) {
    case tag_type::none:
      break;
    case tag_type::put_command: {
      data key;
      data val;
      GENERATE(key);
      GENERATE(val);
      x.content = put_command{std::move(key), std::move(val), nil};
      break;
    }
    case tag_type::put_unique_command: {
      data key;
      data val;
      GENERATE(key);
      GENERATE(val);
      x.content
        = put_unique_command{std::move(key), std::move(val), nil, nullptr, 0};
      break;
    }
    case tag_type::erase_command: {
      data key;
      GENERATE(key);
      x.content = erase_command{std::move(key)};
      break;
    }
    case tag_type::add_command: {
      data key;
      data val;
      data::type init_type{};
      GENERATE(key);
      GENERATE(val);
      READ(init_type);
      x.content = add_command{std::move(key), std::move(val), init_type, nil};
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
    case tag_type::snapshot_command: {
      x.content = snapshot_command{nullptr, nullptr};
      break;
    }
    case tag_type::snapshot_sync_command: {
      x.content = snapshot_sync_command{};
      break;
    }
    case tag_type::set_command: {
      std::unordered_map<data, data> xs;
      GENERATE(xs);
      x.content = set_command{std::move(xs)};
      break;
    }
    case tag_type::clear_command: {
      x.content = clear_command{};
      break;
    }
    default:
      return ec::invalid_tag;
  }
  return caf::none;
}

caf::error data_generator::generate(vector& xs) {
  uint32_t size = 0;
  READ(size);
  for (size_t i = 0; i < size; ++i) {
    data value;
    GENERATE(value);
    xs.emplace_back(std::move(value));
  }
  return caf::none;
}

caf::error data_generator::generate(set& xs) {
  uint32_t size = 0;
  READ(size);
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
  READ(size);
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
  READ(size);
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
  READ(string_size);
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
  caf::visit(f, x);
}

void data_generator::shuffle(vector& xs) {
  for (auto& x : xs)
    shuffle(x);
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

} // namespace detail
} // namespace broker
