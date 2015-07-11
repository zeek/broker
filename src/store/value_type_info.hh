#ifndef BROKER_STORE_VALUE_TYPE_INFO_HH
#define BROKER_STORE_VALUE_TYPE_INFO_HH

#include "broker/store/value.hh"
#include <caf/abstract_uniform_type_info.hpp>
#include <caf/serializer.hpp>
#include <caf/deserializer.hpp>

namespace broker { namespace store {

/**
 * (de)serialization logic for type "value".
 */
class value_type_info
        : public caf::abstract_uniform_type_info<value> {
public:

    value_type_info()
		: caf::abstract_uniform_type_info<value>("broker::store::value")
		{}

	void serialize(const void* ptr, caf::serializer* sink) const override
		{
		auto p = reinterpret_cast<const value*>(ptr);
		*sink << p->item;

		if ( p->expiry )
			{
			*sink << true;
			*sink << *p->expiry;
			}
		else
			*sink << false;
		}

	void deserialize(void* ptr, caf::deserializer* source) const override
		{
		auto p = reinterpret_cast<value*>(ptr);
		p->item = source->read<data>(caf::uniform_typeid<data>());

		if ( source->read<bool>() )
			p->expiry = source->read<expiration_time>(
							caf::uniform_typeid<expiration_time>());
		else
			p->expiry = {};
		}
};

} // namespace store
} // namespace broker

#endif // BROKER_STORE_VALUE_TYPE_INFO_HH
