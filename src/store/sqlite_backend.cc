#include "sqlite_backend_impl.hh"
#include "broker/broker.h"
#include "../persistables.hh"
#include "../util/misc.hh"
#include <cstdio>

template <class T>
static std::string to_blob(const T& obj)
	{
	broker::util::persist::save_archive saver;
	save(saver, obj);
	return saver.get();
	}

template <class T>
static T from_blob(const void* blob, size_t num_bytes)
	{
	T rval;
	broker::util::persist::load_archive loader(blob, num_bytes);
	load(loader, &rval);
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
	         "replace into meta(k, v) values('broker_version', '%s');",
	         BROKER_VERSION);

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
	// Need the expiry blob data to stay in scope until query is done.
	auto eblob = e ? to_blob(*e) : std::string{};

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

static bool
update_expiry(const broker::store::sqlite_stmt& stmt,
              const broker::data& k, const broker::data& v,
              const broker::store::expiration_time& e)
	{
	auto g = stmt.guard(sqlite3_reset);
	auto kblob = to_blob(k);
	auto vblob = to_blob(v);
	auto eblob = to_blob(e);

	if ( sqlite3_bind_blob64(stmt, 1, vblob.data(), vblob.size(),
	                         SQLITE_STATIC) != SQLITE_OK ||
	     sqlite3_bind_blob64(stmt, 2, eblob.data(), eblob.size(),
	                         SQLITE_STATIC) != SQLITE_OK ||
	     sqlite3_bind_blob64(stmt, 3, kblob.data(), kblob.size(),
	                         SQLITE_STATIC) != SQLITE_OK )
		return false;

	if ( sqlite3_step(stmt) != SQLITE_DONE )
		return false;

	return true;
	}

static inline broker::store::modification_result
update_entry(const broker::store::sqlite_stmt& update_stmt,
             const broker::store::sqlite_stmt& update_expiry_stmt,
             const broker::data& k, const broker::data& v,
             broker::util::optional<broker::store::expiration_time> e,
             int& last_rc)
	{
	using broker::store::modification_result;

	if ( e )
		{
		if ( ::update_expiry(update_expiry_stmt, k, v, *e) )
			return {modification_result::status::success, e};
		}
	else
		{
		if ( ::update(update_stmt, k, v) )
			return {modification_result::status::success, {}};
		}

	last_rc = 0;
	return {modification_result::status::failure, {}};
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

broker::store::modification_result
broker::store::sqlite_backend::do_increment(const data& k, int64_t by,
                                            double mod_time)
	{
	auto op = do_lookup_expiry(k);

	if ( ! op )
		{
		pimpl->last_rc = 0;
		return {modification_result::status::failure, {}};
		}

	if ( ! op->first )
		{
		if ( ::insert(pimpl->insert, k, data{by}) )
			return {modification_result::status::success, {}};

		pimpl->last_rc = 0;
		return {modification_result::status::failure, {}};
		}

	auto& v = *op->first;

	if ( ! util::increment_data(v, by, &pimpl->our_last_error) )
		{
		pimpl->last_rc = -1;
		return {modification_result::status::invalid, {}};
		}

	auto new_expiry = util::update_last_modification(op->second, mod_time);
	return ::update_entry(pimpl->update, pimpl->update_expiry, k, v,
	                      std::move(new_expiry), pimpl->last_rc);
	}

broker::store::modification_result
broker::store::sqlite_backend::do_add_to_set(const data& k, data element,
                                             double mod_time)
	{
	auto op = do_lookup_expiry(k);

	if ( ! op )
		{
		pimpl->last_rc = 0;
		return {modification_result::status::failure, {}};
		}

	if ( ! op->first )
		{
		if ( ::insert(pimpl->insert, k, set{std::move(element)}) )
			return {modification_result::status::success, {}};

		pimpl->last_rc = 0;
		return {modification_result::status::failure, {}};
		}

	auto& v = *op->first;

	if ( ! util::add_data_to_set(v, std::move(element),
	                             &pimpl->our_last_error) )
		{
		pimpl->last_rc = -1;
		return {modification_result::status::invalid, {}};
		}

	auto new_expiry = util::update_last_modification(op->second, mod_time);
	return ::update_entry(pimpl->update, pimpl->update_expiry, k, v,
	                      std::move(new_expiry), pimpl->last_rc);
	}

broker::store::modification_result
broker::store::sqlite_backend::do_remove_from_set(const data& k,
                                                  const data& element,
                                                  double mod_time)
	{
	auto op = do_lookup_expiry(k);

	if ( ! op )
		{
		pimpl->last_rc = 0;
		return {modification_result::status::failure, {}};
		}

	if ( ! op->first )
		{
		if ( ::insert(pimpl->insert, k, set{}) )
			return {modification_result::status::success, {}};

		pimpl->last_rc = 0;
		return {modification_result::status::failure, {}};
		}

	auto& v = *op->first;

	if ( ! util::remove_data_from_set(v, element, &pimpl->our_last_error) )
		{
		pimpl->last_rc = -1;
		return {modification_result::status::invalid, {}};
		}

	auto new_expiry = util::update_last_modification(op->second, mod_time);
	return ::update_entry(pimpl->update, pimpl->update_expiry, k, v,
	                      std::move(new_expiry), pimpl->last_rc);
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

bool broker::store::sqlite_backend::do_expire(const data& k,
                                              const expiration_time& expiration)
	{
	const auto& stmt = pimpl->expire;
	auto g = stmt.guard(sqlite3_reset);
	auto kblob = to_blob(k);
	auto eblob = to_blob(expiration);

	if ( sqlite3_bind_blob64(stmt, 1, kblob.data(), kblob.size(),
	                         SQLITE_STATIC) != SQLITE_OK ||
	     sqlite3_bind_blob64(stmt, 2, eblob.data(), eblob.size(),
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

broker::store::modification_result
broker::store::sqlite_backend::do_push_left(const data& k, vector items,
                                            double mod_time)
	{
	auto op = do_lookup_expiry(k);

	if ( ! op )
		{
		pimpl->last_rc = 0;
		return {modification_result::status::failure, {}};
		}

	if ( ! op->first )
		{
		if ( ::insert(pimpl->insert, k, std::move(items)) )
			return {modification_result::status::success, {}};

		pimpl->last_rc = 0;
		return {modification_result::status::failure, {}};
		}

	auto& v = *op->first;

	if ( ! util::push_left(v, std::move(items), &pimpl->our_last_error) )
		{
		pimpl->last_rc = -1;
		return {modification_result::status::invalid, {}};
		}

	auto new_expiry = util::update_last_modification(op->second, mod_time);
	return ::update_entry(pimpl->update, pimpl->update_expiry, k, v,
	                      std::move(new_expiry), pimpl->last_rc);
	}

broker::store::modification_result
broker::store::sqlite_backend::do_push_right(const data& k, vector items,
                                             double mod_time)
	{
	auto op = do_lookup_expiry(k);

	if ( ! op )
		{
		pimpl->last_rc = 0;
		return {modification_result::status::failure, {}};
		}

	if ( ! op->first )
		{
		if ( ::insert(pimpl->insert, k, std::move(items)) )
			return {modification_result::status::success, {}};

		pimpl->last_rc = 0;
		return {modification_result::status::failure, {}};
		}

	auto& v = *op->first;

	if ( ! util::push_right(v, std::move(items), &pimpl->our_last_error) )
		{
		pimpl->last_rc = -1;
		return {modification_result::status::invalid, {}};
		}

	auto new_expiry = util::update_last_modification(op->second, mod_time);
	return ::update_entry(pimpl->update, pimpl->update_expiry, k, v,
	                      std::move(new_expiry), pimpl->last_rc);
	}

std::pair<broker::store::modification_result,
          broker::util::optional<broker::data>>
broker::store::sqlite_backend::do_pop_left(const data& k, double mod_time)
	{
	auto op = do_lookup_expiry(k);

	if ( ! op )
		{
		pimpl->last_rc = 0;
		return {{modification_result::status::failure, {}}, {}};
		}

	if ( ! op->first )
		// Fine, key didn't exist.
		return {{modification_result::status::success, {}}, {}};

	auto& v = *op->first;

	auto rval = util::pop_left(v, &pimpl->our_last_error);

	if ( ! rval )
		{
		pimpl->last_rc = -1;
		return {{modification_result::status::invalid, {}}, {}};
		}

	if ( ! *rval )
		// Fine, popped an empty list.
		return {{modification_result::status::success, {}}, {}};

	auto new_expiry = util::update_last_modification(op->second, mod_time);
	return {::update_entry(pimpl->update, pimpl->update_expiry, k, v,
	                       std::move(new_expiry), pimpl->last_rc),
		    std::move(*rval)};
	}

std::pair<broker::store::modification_result,
          broker::util::optional<broker::data>>
broker::store::sqlite_backend::do_pop_right(const data& k, double mod_time)
	{
	auto op = do_lookup_expiry(k);

	if ( ! op )
		{
		pimpl->last_rc = 0;
		return {{modification_result::status::failure, {}}, {}};
		}

	if ( ! op->first )
		// Fine, key didn't exist.
		return {{modification_result::status::success, {}}, {}};

	auto& v = *op->first;

	auto rval = util::pop_right(v, &pimpl->our_last_error);

	if ( ! rval )
		{
		pimpl->last_rc = -1;
		return {{modification_result::status::invalid, {}}, {}};
		}

	if ( ! *rval )
		// Fine, popped an empty list.
		return {{modification_result::status::success, {}}, {}};

	auto new_expiry = util::update_last_modification(op->second, mod_time);
	return {::update_entry(pimpl->update, pimpl->update_expiry, k, v,
	                       std::move(new_expiry), pimpl->last_rc),
		    std::move(*rval)};
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

broker::util::optional<std::pair<broker::util::optional<broker::data>,
                       broker::util::optional<broker::store::expiration_time>>>
broker::store::sqlite_backend::do_lookup_expiry(const data& k) const
	{
	const auto& stmt = pimpl->lookup_expiry;
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
		return {std::make_pair(util::optional<data>{},
			                   util::optional<expiration_time>{})};

	if ( rc != SQLITE_ROW )
		{
		pimpl->last_rc = 0;
		return {};
		}

	auto val = from_blob<data>(sqlite3_column_blob(stmt, 0),
	                           sqlite3_column_bytes(stmt, 0));

	util::optional<expiration_time> e;

	if ( sqlite3_column_type(stmt, 1) != SQLITE_NULL )
		e = from_blob<expiration_time>(sqlite3_column_blob(stmt, 1),
		                               sqlite3_column_bytes(stmt, 1));

	return {std::make_pair(std::move(val), std::move(e))};
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

// Begin C API
#include "broker/broker.h"
using std::nothrow;

broker_store_sqlite_backend* broker_store_sqlite_backend_create()
	{
	auto rval = new (nothrow) broker::store::sqlite_backend();
	return reinterpret_cast<broker_store_sqlite_backend*>(rval);
	}

void broker_store_sqlite_backend_delete(broker_store_sqlite_backend* b)
	{
	delete reinterpret_cast<broker::store::sqlite_backend*>(b);
	}

int broker_store_sqlite_backend_open(broker_store_sqlite_backend* b,
                                     const char* path)
	{
	auto bb = reinterpret_cast<broker::store::sqlite_backend*>(b);
	return bb->open(path);
	}

int broker_store_sqlite_backend_pragma(broker_store_sqlite_backend* b,
                                       const char* pragma)
	{
	auto bb = reinterpret_cast<broker::store::sqlite_backend*>(b);
	return bb->pragma(pragma);
	}

int broker_store_sqlite_backend_last_error_code(
        const broker_store_sqlite_backend* b)
	{
	auto bb = reinterpret_cast<const broker::store::sqlite_backend*>(b);
	return bb->last_error_code();
	}

const char* broker_store_sqlite_backend_last_error(
        const broker_store_sqlite_backend* b)
	{
	auto bb = reinterpret_cast<const broker::store::sqlite_backend*>(b);
	return bb->last_error().data();
	}
