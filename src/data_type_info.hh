#ifndef BROKER_DATA_TYPE_INFO_HH
#define BROKER_DATA_TYPE_INFO_HH

#include "broker/data.hh"
#include <caf/detail/abstract_uniform_type_info.hpp>
#include <caf/serializer.hpp>
#include <caf/deserializer.hpp>

namespace broker {

class data_type_info : public caf::detail::abstract_uniform_type_info<data> {
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
		auto p = reinterpret_cast<const data*>(ptr);
		*sink << which(*p);
		visit(serializer{sink}, *p);
		}

	void deserialize(void* ptr, caf::deserializer* source) const override
		{
		auto p = reinterpret_cast<data*>(ptr);
		auto tag = source->read<data::tag>(caf::uniform_typeid<data::tag>());
		p->value = data::value_type::make(tag);
		visit(deserializer{source}, *p);
		}
};

} // namespace broker

#endif // BROKER_DATA_TYPE_INFO_HH
