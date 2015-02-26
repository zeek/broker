#include "persistables.hh"

BROKER_PERSIST_VERSION(broker::data, 0)
BROKER_PERSIST_VERSION(broker::store::expiration_time, 0)

namespace broker {

struct data_saver {
	using result_type = void;

	template <typename T>
	result_type operator()(T t)
		{ save(ar, t); }

	result_type operator()(const std::string& b)
		{ save_binary(ar, b.data(), b.size()); }

	result_type operator()(const address& b)
		{
		if ( b.is_v4() )
			save_binary(ar, b.bytes().data() + 12, b.bytes().size() - 12);
		else
			save_binary(ar, b.bytes().data(), b.bytes().size());
		}

	result_type operator()(const subnet& b)
		{
		operator()(b.network());
		save(ar, b.length());
		}

	result_type operator()(const port& b)
		{
		save(ar, b.number());
		auto proto = static_cast<uint8_t>(b.type());
		save(ar, proto);
		}

	result_type operator()(const time_point& b)
		{ save(ar, b.value); }

	result_type operator()(const time_duration& b)
		{ save(ar, b.value); }

	result_type operator()(const enum_value& b)
		{ save_binary(ar, b.name.data(), b.name.size()); }

	result_type operator()(const set& b)
		{
		save_sequence(ar, b.size());

		for ( const auto& d : b )
			save(ar, d, version);
		}

	result_type operator()(const table& b)
		{
		save_sequence(ar, b.size());

		for ( const auto& d : b )
			{
			save(ar, d.first, version);
			save(ar, d.second, version);
			}
		}

	result_type operator()(const vector& b)
		{
		save_sequence(ar, b.size());

		for ( const auto& d : b )
			save(ar, d, version);
		}

	result_type operator()(const record& b)
		{
		save_sequence(ar, b.size());

		for ( const auto& d : b.fields )
			{
			if ( d )
				{
				save(ar, true);
				save(ar, *d, version);
				}
			else
				save(ar, false);
			}
		}

	util::persist::save_archive& ar;
	uint32_t version;
};

struct data_loader {
	using result_type = void;

	template <typename T>
	result_type operator()(T& t)
		{ load(ar, &t); }

	result_type operator()(std::string& b)
		{ load_binary(ar, &b); }

	result_type operator()(address& b)
		{
		std::string network;
		load_binary(ar, &network);
		auto fam = network.size() == 4 ? address::family::ipv4
		                               : address::family::ipv6;
		b = address(reinterpret_cast<const uint32_t*>(network.data()), fam,
		            address::byte_order::network);
		}

	result_type operator()(subnet& b)
		{
		address network;
		operator()(network);
		uint8_t length;
		load(ar, &length);
		b = subnet(std::move(network), length);
		}

	result_type operator()(port& b)
		{
		port::number_type num;
		uint8_t proto;
		load(ar, &num);
		load(ar, &proto);
		b = port(num, static_cast<port::protocol>(proto));
		}

	result_type operator()(time_point& b)
		{ load(ar, &b.value); }

	result_type operator()(time_duration& b)
		{ load(ar, &b.value); }

	result_type operator()(enum_value& b)
		{ load_binary(ar, &b.name); }

	result_type operator()(set& b)
		{
		auto num_items = load_sequence(ar);

		for ( auto i = 0u; i < num_items; ++i )
			{
			data item;
			load(ar, item, version);
			b.emplace(std::move(item));
			}
		}

	result_type operator()(table& b)
		{
		auto num_items = load_sequence(ar);

		for ( auto i = 0u; i < num_items; ++i )
			{
			data key;
			data val;
			load(ar, key, version);
			load(ar, val, version);
			b.emplace(std::move(key), std::move(val));
			}
		}

	result_type operator()(vector& b)
		{
		auto num_items = load_sequence(ar);
		b.reserve(num_items);

		for ( auto i = 0u; i < num_items; ++i )
			{
			data item;
			load(ar, item, version);
			b.emplace_back(std::move(item));
			}
		}

	result_type operator()(record& b)
		{
		auto num_items = load_sequence(ar);
		b.fields.reserve(num_items);

		for ( auto i = 0u; i < num_items; ++i )
			{
			bool exists;
			load(ar, &exists);

			if ( exists )
				{
				data item;
				load(ar, item, version);
				b.fields.emplace_back(std::move(item));
				}
			else
				b.fields.emplace_back(record::field{});
			}
		}

	util::persist::load_archive& ar;
	uint32_t version;
};

void save(util::persist::save_archive& ar, const data& d, uint32_t version)
	{
	auto tag = static_cast<uint8_t>(which(d));
	save(ar, tag);
	visit(data_saver{ar, version}, d);
	}

void load(util::persist::load_archive& ar, data& d, uint32_t version)
	{
	uint8_t tag;
	load(ar, &tag);
	d.value = data::types::make(static_cast<data::tag>(tag));
	visit(data_loader{ar, version}, d);
	}

namespace store {

void save(util::persist::save_archive& ar, const expiration_time& d,
          uint32_t version)
	{
	auto tag = static_cast<uint8_t>(d.type);
	save(ar, tag);
	save(ar, d.expiry_time);

	if ( d.type == expiration_time::tag::since_last_modification )
		save(ar, d.modification_time);
	}

void load(util::persist::load_archive& ar, expiration_time& d, uint32_t version)
	{
	uint8_t tag;
	load(ar, &tag);
	d.type = static_cast<expiration_time::tag>(tag);
	load(ar, &d.expiry_time);

	if ( d.type == expiration_time::tag::since_last_modification )
		load(ar, &d.modification_time);
	}

} // namespace store
} // namespace broker
