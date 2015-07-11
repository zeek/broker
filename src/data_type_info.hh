#ifndef BROKER_DATA_TYPE_INFO_HH
#define BROKER_DATA_TYPE_INFO_HH

#include "broker/data.hh"
#include <caf/abstract_uniform_type_info.hpp>
#include <caf/serializer.hpp>
#include <caf/deserializer.hpp>

namespace broker {

/**
 * (de)serialization logic for type "data".
 */
class data_type_info : public caf::abstract_uniform_type_info<data> {
public:

	data_type_info()
		: caf::abstract_uniform_type_info<data>("broker::data")
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
		auto p = reinterpret_cast<const data*>(ptr);
		*sink << which(*p);
		visit(serializer{sink}, *p);
		}

	void deserialize(void* ptr, caf::deserializer* source) const override
		{
		auto p = reinterpret_cast<data*>(ptr);
		auto tag = source->read<data::tag>(caf::uniform_typeid<data::tag>());
		p->value = data::types::make(tag);
		visit(deserializer{source}, *p);
		}
};

/**
 * (de)serialization logic for type "optional<data>".
 */
class optional_data_type_info :
      public caf::abstract_uniform_type_info<util::optional<data>> {
public:

	optional_data_type_info()
		: caf::abstract_uniform_type_info<util::optional<data>>(
	          "broker::util::optional<data>")
		{}

	void serialize(const void* ptr, caf::serializer* sink) const override
		{
		auto p = reinterpret_cast<const util::optional<data>*>(ptr);
		*sink << p->valid();

		if ( p->valid() )
			*sink << p->get();
		}

	void deserialize(void* ptr, caf::deserializer* source) const override
		{
		auto p = reinterpret_cast<util::optional<data>*>(ptr);
		auto have = source->read<bool>();

		if ( have )
			{
			*p = data{};
			caf::uniform_typeid<data>()->deserialize(&p->get(), source);
			}
		else
			*p = util::none;
		}
};

} // namespace broker

#endif // BROKER_DATA_TYPE_INFO_HH
