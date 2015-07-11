#ifndef BROKER_SUBNET_TYPE_INFO_HH
#define BROKER_SUBNET_TYPE_INFO_HH

#include "broker/subnet.hh"
#include <caf/abstract_uniform_type_info.hpp>
#include <caf/serializer.hpp>
#include <caf/deserializer.hpp>

namespace broker {

/**
 * (de)serialization logic for type "subnet".
 */
class subnet_type_info
      : public caf::abstract_uniform_type_info<subnet> {
public :

	subnet_type_info()
		: caf::abstract_uniform_type_info<subnet>("broker::subnet")
		{}

	void serialize(const void* ptr, caf::serializer* sink) const override
		{
		auto p = reinterpret_cast<const subnet*>(ptr);
		*sink << p->net << p->len;
		}

	void deserialize(void* ptr, caf::deserializer* source) const override
		{
		auto p = reinterpret_cast<subnet*>(ptr);
		source->read(p->net, caf::uniform_typeid<address>());
		source->read(p->len);
		}
};

} // namespace broker

#endif // BROKER_SUBNET_TYPE_INFO_HH
