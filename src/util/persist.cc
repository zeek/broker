#include "persist.hh"
#include <algorithm>
#include <typeindex>
#include <cassert>

static constexpr bool is_big_endian()
	{
#ifdef BROKER_BIG_ENDIAN
	return true;
#else
	return false;
#endif
	}

namespace broker {
namespace util {
namespace persist {

using version_map_type = std::unordered_map<std::type_index, uint32_t>;

class save_archive::impl {
public:

	std::string serial;
	version_map_type version_map;
};

save_archive::save_archive()
	: pimpl(new impl)
	{}

save_archive::~save_archive() = default;

save_archive::save_archive(const save_archive& other)
	: pimpl(new impl(*other.pimpl))
	{}

save_archive::save_archive(save_archive&& other) = default;

save_archive& save_archive::operator=(save_archive other)
	{
	swap(other);
	return *this;
	}

save_archive::save_archive(std::string arg_serial)
	: pimpl(new impl{arg_serial, version_map_type{}})
	{}

void save_archive::reset(std::string arg_serial)
	{
	pimpl->serial = std::move(arg_serial);
	pimpl->version_map.clear();
	}

std::string save_archive::get()
	{
	auto rval = std::move(pimpl->serial);
	reset();
	return rval;
	}

void save_archive::swap(save_archive& other)
	{
	using std::swap;
	swap(pimpl->serial, other.pimpl->serial);
	swap(pimpl->version_map, other.pimpl->version_map);
	}

void save_archive::save_bytes(const uint8_t* bytes, size_t size)
	{
	pimpl->serial.reserve(pimpl->serial.size() + size);
	std::copy(bytes, bytes + size, std::back_inserter(pimpl->serial));
	}

void save_archive::save_bytes_reverse(const uint8_t* bytes, size_t size)
	{
	pimpl->serial.reserve(pimpl->serial.size() + size);
	std::reverse_copy(bytes, bytes + size, std::back_inserter(pimpl->serial));
	}

void save_archive::save_value(const uint8_t* bytes, size_t size)
	{
	if ( is_big_endian() )
		save_bytes_reverse(bytes, size);
	else
		save_bytes(bytes, size);
	}

uint32_t save_archive::register_class(const std::type_info& ti, uint32_t ver)
	{
	auto res = pimpl->version_map.emplace(std::type_index(ti), ver);

	if ( res.second )
		save(*this, ver);

	return ver;
	}

save_archive& save_binary(save_archive& ar, const void* bytes, size_t size)
	{
	auto num_bytes = static_cast<uint64_t>(size);
	save(ar, num_bytes);
	ar.save_bytes(reinterpret_cast<const uint8_t*>(bytes), size);
	return ar;
	}

save_archive& save_sequence(save_archive& ar, size_t size)
	{
	auto num_items = static_cast<uint64_t>(size);
	save(ar, num_items);
	return ar;
	}

class load_archive::impl {
public:

	const void* serial_bytes;
	size_t num_bytes;
	size_t position;
	std::unordered_map<std::type_index, uint32_t> version_map;
};

load_archive::load_archive()
	: pimpl(new impl)
	{}

load_archive::~load_archive() = default;

load_archive::load_archive(const load_archive& other)
	: pimpl(new impl(*other.pimpl))
	{}

load_archive::load_archive(load_archive&& other) = default;

load_archive& load_archive::operator=(load_archive other)
	{
	swap(other);
	return *this;
	}

load_archive::load_archive(const void* bytes, size_t num_bytes)
	: pimpl(new impl{bytes, num_bytes, 0, version_map_type{}})
	{}

void load_archive::reset(const void* bytes, size_t num_bytes)
	{
	pimpl->serial_bytes = bytes;
	pimpl->num_bytes = num_bytes;
	pimpl->position = 0;
	pimpl->version_map.clear();
	}

void load_archive::swap(load_archive& other)
	{
	using std::swap;
	swap(pimpl->serial_bytes, other.pimpl->serial_bytes);
	swap(pimpl->num_bytes, other.pimpl->num_bytes);
	swap(pimpl->position, other.pimpl->position);
	swap(pimpl->version_map, other.pimpl->version_map);
	}

load_archive& load_binary(load_archive& ar, std::string* rval)
	{
	auto size = ar.load_value<uint64_t>();
	assert(ar.pimpl->position + size <= ar.pimpl->num_bytes);
	rval->reserve(rval->size() + size);
	auto src = reinterpret_cast<const uint8_t*>(ar.pimpl->serial_bytes);
	src += ar.pimpl->position;
	std::copy(src, src + size, std::back_inserter(*rval));
	ar.pimpl->position += size;
	return ar;
	}

uint64_t load_sequence(load_archive& ar)
	{
	uint64_t rval;
	load(ar, &rval);
	return rval;
	}

void load_archive::load_value(uint8_t* dst, size_t size)
	{
	assert(pimpl->position + size <= pimpl->num_bytes);
	auto src = reinterpret_cast<const uint8_t*>(pimpl->serial_bytes);
	src += pimpl->position;

	if ( is_big_endian() )
		std::reverse_copy(src, src + size, dst);
	else
		std::copy(src, src + size, dst);

	pimpl->position += size;
	}

uint32_t load_archive::register_class(const std::type_info& ti)
	{
	auto it = pimpl->version_map.find(std::type_index(ti));

	if ( it == pimpl->version_map.end() )
		{
		auto rval = load_value<uint32_t>();
		pimpl->version_map[std::type_index(ti)] = rval;
		return rval;
		}
	else
		return it->second;
	}

} // namespace persist
} // namespace util
} // namespace broker
