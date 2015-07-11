#ifndef BROKER_PORT_TYPE_INFO_HH
#define BROKER_PORT_TYPE_INFO_HH

#include "broker/port.hh"
#include <type_traits>
#include <caf/abstract_uniform_type_info.hpp>
#include <caf/serializer.hpp>
#include <caf/deserializer.hpp>

namespace broker {

/**
 * (de)serialization logic for type "port".
 */
class port_type_info : public caf::abstract_uniform_type_info<port> {
public :

	port_type_info()
		: caf::abstract_uniform_type_info<port>("broker::port")
		{}

	void serialize(const void* ptr, caf::serializer* sink) const override
		{
		using ut = std::underlying_type<port::protocol>::type;
		auto p = reinterpret_cast<const port*>(ptr);
		*sink << p->num << static_cast<ut>(p->proto);
		}

	void deserialize(void* ptr, caf::deserializer* source) const override
		{
		using ut = std::underlying_type<port::protocol>::type;
		auto p = reinterpret_cast<port*>(ptr);
		source->read(p->num);
		p->proto = static_cast<port::protocol>(source->read<ut>());
		}
};

} // namespace broker

#endif // BROKER_PORT_TYPE_INFO_HH
