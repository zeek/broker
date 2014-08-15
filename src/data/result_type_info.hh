#ifndef BROKER_DATA_RESULT_TYPE_INFO_HH
#define BROKER_DATA_RESULT_TYPE_INFO_HH

#include "broker/data/result.hh"
#include <caf/detail/abstract_uniform_type_info.hpp>
#include <caf/serializer.hpp>
#include <caf/deserializer.hpp>

namespace broker { namespace data {

class result_type_info
        : public caf::detail::abstract_uniform_type_info<result> {
	void serialize(const void* ptr, caf::serializer* sink) const override
		{
		auto p = reinterpret_cast<const result*>(ptr);
		sink->write_value(
		      static_cast<std::underlying_type<result::type>::type>(p->tag));
		sink->write_value(
		      static_cast<std::underlying_type<result::status>::type>(p->stat));

		switch ( p->tag ) {
		case result::type::exists_val:
			sink->write_value(p->exists);
			break;
		case result::type::size_val:
			sink->write_value(p->size);
			break;
		case result::type::value_val:
			sink->write_value(p->val);
			break;
		case result::type::keys_val:
			sink->begin_sequence(p->keys.size());

			for ( const auto& k : p->keys )
				sink->write_value(k);

			sink->end_sequence();
			break;
		case result::type::snapshot_val:
			sink->begin_sequence(p->snap.datastore.size());

			for ( const auto& elem : p->snap.datastore )
				{
				sink->write_value(elem.first);
				sink->write_value(elem.second);
				}

			sink->end_sequence();
			sink->begin_sequence(p->snap.sn.sequence.size());

			for ( size_t i = 0; i < p->snap.sn.sequence.size(); ++i )
				sink->write_value(p->snap.sn.sequence[i]);

			sink->end_sequence();
			break;
		}
		}

	void deserialize(void* ptr, caf::deserializer* source) const override
		{
		auto p = reinterpret_cast<result*>(ptr);
		using tag_type = std::underlying_type<result::type>::type;
		using status_type = std::underlying_type<result::status>::type;
		auto tag = static_cast<result::type>(source->read<tag_type>());
		auto stat = static_cast<result::status>(source->read<status_type>());

		if ( stat != result::status::success )
			{
			*p = result(stat);
			return;
			}

		switch ( tag ) {
		case result::type::exists_val:
			*p = result(source->read<bool>());
			break;
		case result::type::size_val:
			*p = result(source->read<uint64_t>());
			break;
		case result::type::value_val:
			*p = result(source->read<value>());
			break;
		case result::type::keys_val:
			{
			auto num = source->begin_sequence();
			std::unordered_set<key> keys;

			for ( size_t i = 0; i < num; ++i )
				keys.insert(source->read<key>());

			source->end_sequence();
			*p = result(std::move(keys));
			break;
			}
		case result::type::snapshot_val:
			{
			auto num = source->begin_sequence();
			std::unordered_map<key, value> dstore;

			for ( size_t i = 0; i < num; ++i )
				dstore.insert(std::make_pair(source->read<key>(),
				                             source->read<value>()));

			source->end_sequence();
			num = source->begin_sequence();
			std::vector<uint64_t> sn;

			for ( size_t i = 0; i < num; ++i )
				sn.push_back(source->read<uint64_t>());

			*p = result(snapshot{std::move(dstore),
			                     sequence_num(std::move(sn))});
			break;
			}
		}
		}
};

} // namespace data
} // namespace broker

#endif // BROKER_DATA_RESULT_TYPE_INFO_HH
