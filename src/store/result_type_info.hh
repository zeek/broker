#ifndef BROKER_STORE_RESULT_TYPE_INFO_HH
#define BROKER_STORE_RESULT_TYPE_INFO_HH

#include "broker/store/result.hh"
#include <caf/abstract_uniform_type_info.hpp>
#include <caf/serializer.hpp>
#include <caf/deserializer.hpp>

namespace broker { namespace store {

/**
 * (de)serialization logic for type "result".
 */
class result_type_info
        : public caf::abstract_uniform_type_info<result> {
public:

	result_type_info()
		: caf::abstract_uniform_type_info<result>(
	          "broker::store::result")
		{}

	struct serializer {
		using result_type = void;

		template <typename T>
		result_type operator()(const T& m) const
			{ *sink << m; }

		caf::serializer* sink;
	};

	struct deserializer {
		using result_type = void;

		template <typename T>
		result_type operator()(T& m) const
			{ caf::uniform_typeid<T>()->deserialize(&m, source); }

		caf::deserializer* source;
	};

	void serialize(const void* ptr, caf::serializer* sink) const override
		{
		auto p = reinterpret_cast<const result*>(ptr);
		*sink << p->stat;

		if ( p->stat != result::status::success )
			return;

		*sink << p->value.which();
		visit(serializer{sink}, p->value);
		}

	void deserialize(void* ptr, caf::deserializer* source) const override
		{
		auto p = reinterpret_cast<result*>(ptr);
		auto stat = source->read<result::status>(
		                caf::uniform_typeid<result::status>());

		if ( stat != result::status::success )
			{
			*p = result(stat);
			return;
			}

		auto tag = source->read<result::tag>(
		               caf::uniform_typeid<result::tag>());
		auto rd = result::type::make(tag);
		visit(deserializer{source}, rd);
		*p = result(std::move(rd));
		}
};

} // namespace store
} // namespace broker

#endif // BROKER_STORE_RESULT_TYPE_INFO_HH
