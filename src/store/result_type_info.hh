#ifndef BROKER_STORE_RESULT_TYPE_INFO_HH
#define BROKER_STORE_RESULT_TYPE_INFO_HH

#include "broker/store/result.hh"
#include <caf/detail/abstract_uniform_type_info.hpp>
#include <caf/serializer.hpp>
#include <caf/deserializer.hpp>

namespace broker { namespace store {

class result_type_info
        : public caf::detail::abstract_uniform_type_info<result> {
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

		*sink <<
		      static_cast<std::underlying_type<result::status>::type>(p->stat);

		if ( p->stat != result::status::success )
			return;

		*sink << static_cast<std::underlying_type<result::type>::type>(
		             p->value.which());

		visit(serializer{sink}, p->value);
		}

	void deserialize(void* ptr, caf::deserializer* source) const override
		{
		auto p = reinterpret_cast<result*>(ptr);
		using tag_type = std::underlying_type<result::type>::type;
		using status_type = std::underlying_type<result::status>::type;
		auto stat = static_cast<result::status>(source->read<status_type>());

		if ( stat != result::status::success )
			{
			*p = result(stat);
			return;
			}

		auto tag = static_cast<result::type>(source->read<tag_type>());
		auto rd = result::result_data::make(tag);
		visit(deserializer{source}, rd);
		*p = result(std::move(rd));
		}
};

} // namespace store
} // namespace broker

#endif // BROKER_STORE_RESULT_TYPE_INFO_HH
