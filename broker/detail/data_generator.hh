#ifndef BROKER_DETAIL_DATA_GENERATOR_HH
#define BROKER_DETAIL_DATA_GENERATOR_HH

#include <random>

#include <caf/error.hpp>
#include <caf/fwd.hpp>

#include "broker/data.hh"

namespace broker {
namespace detail {

/// Generates random Broker ::data from recorded meta data.
class data_generator {
public:
  /// Helper class for filling values with random content.
  struct mixer {
    data_generator& generator;

    template <class T>
    void operator()(T& x) {
      generator.shuffle(x);
    }
  };

  data_generator(caf::binary_deserializer& meta_data_source, size_t seed = 0);

  caf::error operator()(data& x);

  caf::error generate(data& x);

  caf::error generate(data::type tag, data& x);

  caf::error generate(vector& xs);

  caf::error generate(set& xs);

  caf::error generate(table& xs);

  template <class T>
  caf::error generate(T& x) {
    shuffle(x);
    return caf::none;
  }

  caf::error generate(std::string& x);

  caf::error generate(enum_value& x);

  void shuffle(none&);

  void shuffle(boolean& x);

  template <class T>
  typename std::enable_if<std::is_arithmetic<T>::value>::type shuffle(T& x) {
    x = engine_();
  }

  void shuffle(std::string& x);

  void shuffle(enum_value& x);

  void shuffle(port& x);

  void shuffle(address& x);

  void shuffle(subnet& x);

  void shuffle(timespan& x);

  void shuffle(timestamp& x);

  void shuffle(data& x);

  void shuffle(vector& xs);

  void shuffle(set&);

  void shuffle(table& xs);

private:
  caf::binary_deserializer& source_;
  std::minstd_rand engine_;
  std::uniform_int_distribution<char> char_generator_;
  std::uniform_int_distribution<uint8_t> byte_generator_;
};

} // namespace detail
} // namespace broker

#endif // BROKER_DETAIL_DATA_GENERATOR_HH
