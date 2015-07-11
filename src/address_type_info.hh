#ifndef BROKER_ADDRESS_TYPE_INFO_HH
#define BROKER_ADDRESS_TYPE_INFO_HH

#include "broker/address.hh"
#include <caf/abstract_uniform_type_info.hpp>
#include <caf/serializer.hpp>
#include <caf/deserializer.hpp>

namespace broker {

/**
 * (de)serialization logic for type "address".
 */
class address_type_info : public caf::abstract_uniform_type_info<address> {
public :

	address_type_info()
		: caf::abstract_uniform_type_info<address>("broker::address")
		{}

	void serialize(const void* ptr, caf::serializer* sink) const override
		{
		auto p = reinterpret_cast<const address*>(ptr);
		*sink << *reinterpret_cast<const uint64_t*>(&p->addr[0]);
		*sink << *reinterpret_cast<const uint64_t*>(&p->addr[8]);
		}

	void deserialize(void* ptr, caf::deserializer* source) const override
		{
		auto p = reinterpret_cast<address*>(ptr);
		source->read(*reinterpret_cast<uint64_t*>(&p->addr[0]));
		source->read(*reinterpret_cast<uint64_t*>(&p->addr[8]));
		}
};

} // namespace broker

#endif // BROKER_ADDRESS_TYPE_INFO_HH
