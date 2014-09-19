#ifndef BROKER_DATA_HH
#define BROKER_DATA_HH

#include <broker/util/variant.hh>
#include <string>

namespace broker {

// TODO: add more types

enum class data_type : uint8_t {
	int_data,
	string_data,
};

using data = util::variant<data_type,
                           int,
                           std::string>;

} // namespace broker

#endif // BROKER_DATA_HH
