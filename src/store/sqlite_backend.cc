#include "sqlite_backend_impl.hh"
#include "broker/broker.h"
#include "../util/misc.hh"
#include <cstdio>
#include <caf/binary_serializer.hpp>
#include <caf/binary_deserializer.hpp>

template <class T>
static std::vector<unsigned char> to_blob(const T& obj)
	{
	std::vector<unsigned char> rval;
	caf::binary_serializer bs(std::back_inserter(rval));
	bs << obj;
	return rval;
	}

template <class T>
static T from_blob(const void* blob, size_t num_bytes)
	{
	T rval;
	caf::binary_deserializer bd(blob, num_bytes);
	caf::uniform_typeid<T>()->deserialize(&rval, &bd);
	return rval;
	}

static bool initialize(sqlite3* db)
	{
	if ( sqlite3_exec(db,
	                  "create table if not exists "
	                  "meta(k text primary key, v text);",
	                  nullptr, nullptr, nullptr) != SQLITE_OK )
		return false;

	if ( sqlite3_exec(db,
	                  "create table if not exists "
	                  "store(k blob primary key, v blob, expiry blob);",
	                  nullptr, nullptr, nullptr) != SQLITE_OK )
		return false;

	char tmp[128];
	snprintf(tmp, sizeof(tmp),
	         "replace into meta(k, v) values('broker_version', '%d.%d.%d');",
	         BROKER_VERSION_MAJOR, BROKER_VERSION_MINOR, BROKER_VERSION_PATCH);

	if ( sqlite3_exec(db, tmp, nullptr, nullptr, nullptr) != SQLITE_OK )
		return false;

	return true;
	}

static bool
insert(const broker::store::sqlite_stmt& stmt,
       const broker::data& k, const broker::data& v,
       const broker::util::optional<broker::store::expiration_time>& e = {})
	{
	auto g = stmt.guard(sqlite3_reset);
	auto kblob = to_blob(k);
	auto vblob = to_blob(v);
	auto eblob = e ? to_blob(*e) : std::vector<unsigned char>{};

	if ( sqlite3_bind_blob64(stmt, 1, kblob.data(), kblob.size(),
	                         SQLITE_STATIC) != SQLITE_OK ||
	     sqlite3_bind_blob64(stmt, 2, vblob.data(), vblob.size(),
	                         SQLITE_STATIC) != SQLITE_OK )
		return false;

	if ( e )
		{
		if ( sqlite3_bind_blob64(stmt, 3, eblob.data(), eblob.size(),
		                         SQLITE_STATIC) != SQLITE_OK )
			return false;
		}
	else
		{
		if ( sqlite3_bind_null(stmt, 3) != SQLITE_OK )
			return false;
		}

	if ( sqlite3_step(stmt) != SQLITE_DONE )
		return false;

	return true;
	}

static bool
update(const broker::store::sqlite_stmt& stmt,
       const broker::data& k, const broker::data& v)
	{
	auto g = stmt.guard(sqlite3_reset);
	auto kblob = to_blob(k);
	auto vblob = to_blob(v);

	if ( sqlite3_bind_blob64(stmt, 1, vblob.data(), vblob.size(),
	                         SQLITE_STATIC) != SQLITE_OK ||
	     sqlite3_bind_blob64(stmt, 2, kblob.data(), kblob.size(),
	                         SQLITE_STATIC) != SQLITE_OK )
		return false;

	if ( sqlite3_step(stmt) != SQLITE_DONE )
		return false;

	return true;
	}

broker::store::sqlite_backend::sqlite_backend()
    : backend(), pimpl(new impl)
	{}

broker::store::sqlite_backend::~sqlite_backend() = default;

broker::store::sqlite_backend::sqlite_backend(sqlite_backend&&) = default;

broker::store::sqlite_backend&
broker::store::sqlite_backend::operator=(sqlite_backend&&) = default;


bool broker::store::sqlite_backend::open(std::string db_path,
                                         std::deque<std::string> pragmas)
	{
	pimpl->last_rc = 0;

	if ( sqlite3_open(db_path.c_str(), &pimpl->db) != SQLITE_OK )
		{
		sqlite3_close(pimpl->db);
		pimpl->db = nullptr;
		return false;
		}

	if ( ! initialize(pimpl->db) )
		return false;

	if ( ! pimpl->prepare_statements() )
		return false;

	for ( auto& p : pragmas )
		{
		if ( ! pragma(std::move(p)) )
			return false;
		}

	return true;
	}

bool broker::store::sqlite_backend::sqlite_backend::pragma(std::string p)
	{
	return sqlite3_exec(pimpl->db, p.c_str(), nullptr, nullptr,
	                    nullptr) == SQLITE_OK;
	}

int broker::store::sqlite_backend::last_error_code() const
	{
	if ( pimpl->last_rc < 0 )
		return pimpl->last_rc;
	return sqlite3_errcode(pimpl->db);
	}

void broker::store::sqlite_backend::do_increase_sequence()
	{ ++pimpl->sn; }

std::string broker::store::sqlite_backend::do_last_error() const
	{
	if ( pimpl->last_rc < 0 )
		return pimpl->our_last_error;
	return sqlite3_errmsg(pimpl->db);
	}

bool broker::store::sqlite_backend::do_init(snapshot sss)
	{
	if ( ! clear() )
		return false;

	for ( const auto& e : sss.entries )
		{
		if ( ! ::insert(pimpl->insert, e.first, e.second.item,
		                e.second.expiry) )
			{
			pimpl->last_rc = 0;
			return false;
			}
		}

	pimpl->sn = std::move(sss.sn);
	return true;
	}

const broker::store::sequence_num&
broker::store::sqlite_backend::do_sequence() const
	{ return pimpl->sn; }

bool broker::store::sqlite_backend::do_insert(data k, data v,
                                              util::optional<expiration_time> e)
	{
	if ( ! ::insert(pimpl->insert, k, v, e) )
		{
		pimpl->last_rc = 0;
		return false;
		}

	return true;
	}

int broker::store::sqlite_backend::do_increment(const data& k, int64_t by)
	{
	auto oov = do_lookup(k);

	if ( ! oov )
		{
		pimpl->last_rc = 0;
		return -1;
		}

	auto& ov = *oov;

	if ( ! ov )
		{
		if ( ::insert(pimpl->insert, k, data{by}) )
			return 0;

		pimpl->last_rc = 0;
		return -1;
		}

	auto& v = *ov;

	if ( ! util::increment_data(v, by, &pimpl->our_last_error) )
		{
		pimpl->last_rc = -1;
		return 1;
		}

	if ( ::update(pimpl->update, k, v) )
		return 0;

	pimpl->last_rc = 0;
	return -1;
	}

int broker::store::sqlite_backend::do_add_to_set(const data& k, data element)
	{
	auto oov = do_lookup(k);

	if ( ! oov )
		{
		pimpl->last_rc = 0;
		return -1;
		}

	auto& ov = *oov;

	if ( ! ov )
		{
		if ( ::insert(pimpl->insert, k, set{std::move(element)}) )
			return 0;

		pimpl->last_rc = 0;
		return -1;
		}

	auto& v = *ov;

	if ( ! util::add_data_to_set(v, std::move(element),
	                             &pimpl->our_last_error) )
		{
		pimpl->last_rc = -1;
		return 1;
		}

	if ( ::update(pimpl->update, k, v) )
		return 0;

	pimpl->last_rc = 0;
	return -1;
	}

int broker::store::sqlite_backend::do_remove_from_set(const data& k,
                                                      const data& element)
	{
	auto oov = do_lookup(k);

	if ( ! oov )
		{
		pimpl->last_rc = 0;
		return -1;
		}

	auto& ov = *oov;

	if ( ! ov )
		{
		if ( ::insert(pimpl->insert, k, set{}) )
			return 0;

		pimpl->last_rc = 0;
		return -1;
		}

	auto& v = *ov;

	if ( ! util::remove_data_from_set(v, element, &pimpl->our_last_error) )
		{
		pimpl->last_rc = -1;
		return 1;
		}

	if ( ::update(pimpl->update, k, v) )
		return 0;

	pimpl->last_rc = 0;
	return -1;
	}

bool broker::store::sqlite_backend::do_erase(const data& k)
	{
	const auto& stmt = pimpl->erase;
	auto g = stmt.guard(sqlite3_reset);
	auto kblob = to_blob(k);

	if ( sqlite3_bind_blob64(stmt, 1, kblob.data(), kblob.size(),
	                         SQLITE_STATIC) != SQLITE_OK )
		{
		pimpl->last_rc = 0;
		return false;
		}

	if ( sqlite3_step(stmt) != SQLITE_DONE )
		{
		pimpl->last_rc = 0;
		return false;
		}

	return true;
	}

bool broker::store::sqlite_backend::do_clear()
	{
	const auto& stmt = pimpl->clear;
	auto g = stmt.guard(sqlite3_reset);

	if ( sqlite3_step(stmt) != SQLITE_DONE )
		{
		pimpl->last_rc = 0;
		return false;
		}

	return true;
	}

broker::util::optional<broker::util::optional<broker::data>>
broker::store::sqlite_backend::do_lookup(const data& k) const
	{
	const auto& stmt = pimpl->lookup;
	auto g = stmt.guard(sqlite3_reset);
	auto kblob = to_blob(k);

	if ( sqlite3_bind_blob64(stmt, 1, kblob.data(), kblob.size(),
	                         SQLITE_STATIC) != SQLITE_OK )
		{
		pimpl->last_rc = 0;
		return {};
		}

	auto rc = sqlite3_step(stmt);

	if ( rc == SQLITE_DONE )
		return util::optional<data>{};

	if ( rc != SQLITE_ROW )
		{
		pimpl->last_rc = 0;
		return {};
		}

	return {from_blob<data>(sqlite3_column_blob(stmt, 0),
	                        sqlite3_column_bytes(stmt, 0))};
	}

broker::util::optional<bool>
broker::store::sqlite_backend::do_exists(const data& k) const
	{
	const auto& stmt = pimpl->exists;
	auto g = stmt.guard(sqlite3_reset);
	auto kblob = to_blob(k);

	if ( sqlite3_bind_blob64(stmt, 1, kblob.data(), kblob.size(),
	                         SQLITE_STATIC) != SQLITE_OK )
		{
		pimpl->last_rc = 0;
		return {};
		}

	auto rc = sqlite3_step(stmt);

	if ( rc == SQLITE_DONE )
		return false;

	if ( rc == SQLITE_ROW )
		return true;

	pimpl->last_rc = 0;
	return {};
	}

broker::util::optional<std::vector<broker::data>>
broker::store::sqlite_backend::do_keys() const
	{
	const auto& stmt = pimpl->keys;
	auto g = stmt.guard(sqlite3_reset);
	std::vector<data> rval;
	int rc;

	while ( (rc = sqlite3_step(stmt)) == SQLITE_ROW )
		rval.emplace_back(from_blob<data>(sqlite3_column_blob(stmt, 0),
		                                  sqlite3_column_bytes(stmt, 0)));

	if ( rc == SQLITE_DONE )
		return rval;

	pimpl->last_rc = 0;
	return {};
	}

broker::util::optional<uint64_t> broker::store::sqlite_backend::do_size() const
	{
	const auto& stmt = pimpl->size;
	auto g = stmt.guard(sqlite3_reset);
	auto rc = sqlite3_step(stmt);

	if ( rc == SQLITE_DONE )
		{
		pimpl->last_rc = -1;
		pimpl->our_last_error = "size query returned no result";
		return {};
		}

	if ( rc != SQLITE_ROW )
		{
		pimpl->last_rc = 0;
		return {};
		}

	return sqlite3_column_int(stmt, 0);
	}

broker::util::optional<broker::store::snapshot>
broker::store::sqlite_backend::do_snap() const
	{
	const auto& stmt = pimpl->snap;
	auto g = stmt.guard(sqlite3_reset);
	snapshot rval;
	rval.sn = pimpl->sn;
	int rc;

	while ( (rc = sqlite3_step(stmt)) == SQLITE_ROW )
		{
		auto k = from_blob<data>(sqlite3_column_blob(stmt, 0),
		                         sqlite3_column_bytes(stmt, 0));
		auto v = from_blob<data>(sqlite3_column_blob(stmt, 1),
		                         sqlite3_column_bytes(stmt, 1));
		util::optional<expiration_time> e;

		if ( sqlite3_column_type(stmt, 2) != SQLITE_NULL )
			e = from_blob<expiration_time>(sqlite3_column_blob(stmt, 2),
			                               sqlite3_column_bytes(stmt, 2));

		auto entry = std::make_pair(std::move(k),
		                            value{std::move(v), std::move(e)});
		rval.entries.emplace_back(std::move(entry));
		}

	if ( rc == SQLITE_DONE )
		return rval;

	pimpl->last_rc = 0;
	return {};
	}

broker::util::optional<std::deque<broker::store::expirable>>
broker::store::sqlite_backend::do_expiries() const
	{
	const auto& stmt = pimpl->expiries;
	auto g = stmt.guard(sqlite3_reset);
	std::deque<expirable> rval;
	int rc;

	while ( (rc = sqlite3_step(stmt)) == SQLITE_ROW )
		{
		auto k = from_blob<data>(sqlite3_column_blob(stmt, 0),
		                         sqlite3_column_bytes(stmt, 0));
		auto e = from_blob<expiration_time>(sqlite3_column_blob(stmt, 1),
		                                    sqlite3_column_bytes(stmt, 1));
		rval.emplace_back(expirable{std::move(k), std::move(e)});
		}

	if ( rc == SQLITE_DONE )
		return rval;

	pimpl->last_rc = 0;
	return {};
	}
