#ifndef BROKER_PEERING_TYPE_INFO_HH
#define BROKER_PEERING_TYPE_INFO_HH

#include "peering_impl.hh"
#include <caf/abstract_uniform_type_info.hpp>
#include <caf/serializer.hpp>
#include <caf/deserializer.hpp>

namespace broker {

/**
 * (de)serialization logic for type "peering".
 */
class peering_type_info : public caf::abstract_uniform_type_info<peering> {
public:

	peering_type_info()
		: caf::abstract_uniform_type_info<peering>("broker::peering")
		{}

	void serialize(const void* ptr, caf::serializer* sink) const override
		{
		auto p = reinterpret_cast<const peering*>(ptr);
		*sink << *p->pimpl.get();
		}

	void deserialize(void* ptr, caf::deserializer* source) const override
		{
		auto p = reinterpret_cast<peering*>(ptr);
		std::unique_ptr<peering::impl> pi(new peering::impl);
		caf::uniform_typeid<peering::impl>()->deserialize(pi.get(), source);
		*p = peering(std::move(pi));
		}
};

} // namespace broker

#endif // BROKER_PEERING_TYPE_INFO_HH
