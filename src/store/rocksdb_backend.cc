#include "rocksdb_backend_impl.hh"
#include "broker/broker.h"
#include "../util/misc.hh"
#include <caf/binary_serializer.hpp>
#include <caf/binary_deserializer.hpp>
#include <rocksdb/env.h>

static std::string version_string()
	{
	char tmp[32];
	snprintf(tmp, sizeof(tmp), "%d.%d.%d",
	         BROKER_VERSION_MAJOR, BROKER_VERSION_MINOR, BROKER_VERSION_PATCH);
	return tmp;
	}

template <class T>
static void to_serial(const T& obj, std::string& rval)
	{
	caf::binary_serializer bs(std::back_inserter(rval));
	bs << obj;
	}

template <class T>
static std::string to_serial(const T& obj)
	{
	std::string rval;
	to_serial(obj, rval);
	return rval;
	}

template <class T>
static std::string to_serial(const T& obj, char keyspace)
	{
	std::string rval{keyspace};
	to_serial(obj, rval);
	return rval;
	}

template <class T>
static T from_serial(const char* blob, size_t num_bytes)
	{
	T rval;
	caf::binary_deserializer bd(blob, num_bytes);
	caf::uniform_typeid<T>()->deserialize(&rval, &bd);
	return rval;
	}

template <class T, class C>
static T from_serial(const C& bytes)
	{ return from_serial<T>(bytes.data(), bytes.size()); }

static rocksdb::Status
insert(rocksdb::DB* db, const broker::data& k, const broker::data& v,
       const broker::util::optional<broker::store::expiration_time>& e = {})
	{
	auto kserial = to_serial(k, 'a');
	auto vserial = to_serial(v);

	if ( ! e )
		return db->Put({}, kserial, vserial);

	auto evserial = to_serial(*e);
	rocksdb::WriteBatch batch;
	batch.Put(kserial, vserial);
	kserial[0] = 'e';
	batch.Put(kserial, evserial);
	return db->Write({}, &batch);
	}

broker::store::rocksdb_backend::rocksdb_backend(uint64_t exact_size_threshold)
	: pimpl(new impl(exact_size_threshold))
	{}

broker::store::rocksdb_backend::~rocksdb_backend() = default;

broker::store::rocksdb_backend::rocksdb_backend(rocksdb_backend&&) = default;

broker::store::rocksdb_backend&
broker::store::rocksdb_backend::operator=(rocksdb_backend&&) = default;

rocksdb::Status
broker::store::rocksdb_backend::open(std::string db_path,
                                     rocksdb::Options options)
	{
	rocksdb::DB* db;
	auto rval = rocksdb::DB::Open(options, db_path, &db);
	pimpl->db.reset(db);
	options.create_if_missing = true;
	pimpl->options = options;

	if ( rval.ok() )
		{
		auto ver = version_string();
		// Use key-space prefix 'm' to store metadata, 'a' for application
		// data, and 'e' for expiration values.
		rval = pimpl->db->Put({}, "mbroker_version", ver);
		return rval;
		}

	return rval;
	}

void broker::store::rocksdb_backend::do_increase_sequence()
	{ ++pimpl->sn; }

std::string broker::store::rocksdb_backend::do_last_error() const
	{ return pimpl->last_error; }

bool broker::store::rocksdb_backend::do_init(snapshot sss)
	{
	if ( ! do_clear() )
		return false;

	rocksdb::WriteBatch batch;

	for ( const auto& kv : sss.entries )
		{
		auto kserial = to_serial(kv.first, 'a');
		auto vserial = to_serial(kv.second.item);
		batch.Put(kserial, vserial);

		if ( kv.second.expiry )
			{
			kserial[0] = 'e';
			auto evserial = to_serial(*kv.second.expiry);
			batch.Put(kserial, evserial);
			}
		}

	pimpl->sn = std::move(sss.sn);
	return pimpl->require_ok(pimpl->db->Write({}, &batch));
	}

const broker::store::sequence_num&
broker::store::rocksdb_backend::do_sequence() const
	{ return pimpl->sn; }

bool
broker::store::rocksdb_backend::do_insert(data k, data v,
                                          util::optional<expiration_time> e)
	{
	if ( ! pimpl->require_db() )
		return false;

	return pimpl->require_ok(::insert(pimpl->db.get(), k, v, e));
	}

int broker::store::rocksdb_backend::do_increment(const data& k, int64_t by)
	{
	if ( pimpl->options.merge_operator )
		{
		auto kserial = to_serial(k, 'a');
		auto vserial = to_serial(by, '+');

		if ( pimpl->require_ok(pimpl->db->Merge({}, kserial, vserial)) )
			return 0;

		return -1;
		}

	auto oov = do_lookup(k);

	if ( ! oov )
		return -1;

	auto& ov = *oov;

	if ( ! util::increment_data(ov, by, &pimpl->last_error) )
		return 1;

	const auto& v = *ov;

	if ( pimpl->require_ok(::insert(pimpl->db.get(), k, v)) )
		return 0;

	return -1;
	}

int broker::store::rocksdb_backend::do_add_to_set(const data& k, data element)
	{
	if ( pimpl->options.merge_operator )
		{
		auto kserial = to_serial(k, 'a');
		auto vserial = to_serial(element, 'a');

		if ( pimpl->require_ok(pimpl->db->Merge({}, kserial, vserial)) )
			return 0;

		return -1;
		}

	auto oov = do_lookup(k);

	if ( ! oov )
		return -1;

	auto& ov = *oov;

	if ( ! util::add_data_to_set(ov, std::move(element), &pimpl->last_error) )
		return 1;

	const auto& v = *ov;

	if ( pimpl->require_ok(::insert(pimpl->db.get(), k, v)) )
		return 0;

	return -1;
	}

int broker::store::rocksdb_backend::do_remove_from_set(const data& k,
                                                       const data& element)
	{
	if ( pimpl->options.merge_operator )
		{
		auto kserial = to_serial(k, 'a');
		auto vserial = to_serial(element, 'r');

		if ( pimpl->require_ok(pimpl->db->Merge({}, kserial, vserial)) )
			return 0;

		return -1;
		}

	auto oov = do_lookup(k);

	if ( ! oov )
		return -1;

	auto& ov = *oov;

	if ( ! util::remove_data_from_set(ov, element, &pimpl->last_error) )
		return 1;

	const auto& v = *ov;

	if ( pimpl->require_ok(::insert(pimpl->db.get(), k, v)) )
		return 0;

	return -1;
	}

bool broker::store::rocksdb_backend::do_erase(const data& k)
	{
	if ( ! pimpl->require_db() )
		return false;

	auto kserial = to_serial(k, 'a');

	if ( ! pimpl->require_ok(pimpl->db->Delete({}, kserial)) )
		return false;

	kserial[0] = 'e';
	return pimpl->require_ok(pimpl->db->Delete({}, kserial));
	}

bool broker::store::rocksdb_backend::do_clear()
	{
	if ( ! pimpl->require_db() )
		return false;

	std::string db_path = pimpl->db->GetName();
	pimpl->db.reset();
	auto stat = rocksdb::DestroyDB(db_path, rocksdb::Options{});

	if ( ! pimpl->require_ok(stat) )
		return false;

	return pimpl->require_ok(open(std::move(db_path), pimpl->options));
	}

int broker::store::rocksdb_backend::do_push_left(const data& k, vector items)
	{
	if ( pimpl->options.merge_operator )
		{
		auto kserial = to_serial(k, 'a');
		auto vserial = to_serial(data{std::move(items)}, 'h');

		if ( pimpl->require_ok(pimpl->db->Merge({}, kserial, vserial)) )
			return 0;

		return -1;
		}

	auto oov = do_lookup(k);

	if ( ! oov )
		return -1;

	auto& ov = *oov;

	if ( ! util::push_left(ov, std::move(items), &pimpl->last_error) )
		return 1;

	const auto& v = *ov;

	if ( pimpl->require_ok(::insert(pimpl->db.get(), k, v)) )
		return 0;

	return -1;
	}

int broker::store::rocksdb_backend::do_push_right(const data& k, vector items)
	{
	if ( pimpl->options.merge_operator )
		{
		auto kserial = to_serial(k, 'a');
		auto vserial = to_serial(data{std::move(items)}, 't');

		if ( pimpl->require_ok(pimpl->db->Merge({}, kserial, vserial)) )
			return 0;

		return -1;
		}

	auto oov = do_lookup(k);

	if ( ! oov )
		return -1;

	auto& ov = *oov;

	if ( ! util::push_right(ov, std::move(items), &pimpl->last_error) )
		return 1;

	const auto& v = *ov;

	if ( pimpl->require_ok(::insert(pimpl->db.get(), k, v)) )
		return 0;

	return -1;
	}

broker::util::optional<broker::util::optional<broker::data>>
broker::store::rocksdb_backend::do_pop_left(const data& k)
	{
	auto oov = do_lookup(k);

	if ( ! oov )
		return {};

	auto& ov = *oov;

	if ( ! ov )
		// Fine, key didn't exist.
		return util::optional<data>{};

	auto& v = *ov;

	auto rval = util::pop_left(v, &pimpl->last_error);

	if ( ! rval )
		return rval;

	if ( ! *rval )
		// Fine, popped an empty list.
		return rval;

	if ( pimpl->require_ok(::insert(pimpl->db.get(), k, v)) )
		return rval;

	return {};
	}

broker::util::optional<broker::util::optional<broker::data>>
broker::store::rocksdb_backend::do_pop_right(const data& k)
	{
	auto oov = do_lookup(k);

	if ( ! oov )
		return {};

	auto& ov = *oov;

	if ( ! ov )
		// Fine, key didn't exist.
		return util::optional<data>{};

	auto& v = *ov;

	auto rval = util::pop_right(v, &pimpl->last_error);

	if ( ! rval )
		return rval;

	if ( ! *rval )
		// Fine, popped an empty list.
		return rval;

	if ( pimpl->require_ok(::insert(pimpl->db.get(), k, v)) )
		return rval;

	return {};
	}

broker::util::optional<broker::util::optional<broker::data>>
broker::store::rocksdb_backend::do_lookup(const data& k) const
	{
	if ( ! pimpl->require_db() )
		return {};

	auto kserial = to_serial(k, 'a');
	std::string vserial;
	bool value_found;

	if ( ! pimpl->db->KeyMayExist({}, kserial, &vserial, &value_found) )
		return util::optional<data>{};

	if ( value_found )
		return {from_serial<data>(vserial)};

	auto stat = pimpl->db->Get(rocksdb::ReadOptions{}, kserial, &vserial);

	if ( stat.IsNotFound() )
		return util::optional<data>{};

	if ( ! pimpl->require_ok(stat) )
		return {};

	return {from_serial<data>(vserial)};
	}

broker::util::optional<bool>
broker::store::rocksdb_backend::do_exists(const data& k) const
	{
	if ( ! pimpl->require_db() )
		return {};

	auto kserial = to_serial(k, 'a');
	std::string vserial;

	if ( ! pimpl->db->KeyMayExist(rocksdb::ReadOptions{}, kserial, &vserial) )
		return false;

	auto stat = pimpl->db->Get(rocksdb::ReadOptions{}, kserial, &vserial);

	if ( stat.IsNotFound() )
		return false;

	if ( ! pimpl->require_ok(stat) )
		return {};

	return true;
	}

broker::util::optional<std::vector<broker::data>>
broker::store::rocksdb_backend::do_keys() const
	{
	if ( ! pimpl->require_db() )
		return {};

	rocksdb::ReadOptions options;
	options.fill_cache = false;
	std::unique_ptr<rocksdb::Iterator> it(pimpl->db->NewIterator(options));
	std::vector<data> rval;

	for ( it->Seek("a"); it->Valid() && it->key()[0] == 'a'; it->Next() )
		{
		auto s = it->key();
		s.remove_prefix(1);
		rval.emplace_back(from_serial<data>(s));
		}

	if ( ! pimpl->require_ok(it->status()) )
		return {};

	return rval;
	}

broker::util::optional<uint64_t> broker::store::rocksdb_backend::do_size() const
	{
	if ( ! pimpl->require_db() )
		return {};

	uint64_t rval;

	if ( pimpl->db->GetIntProperty("rocksdb.estimate-num-keys", &rval) &&
	     rval > pimpl->exact_size_threshold )
		return rval;

	rocksdb::ReadOptions options;
	options.fill_cache = false;
	std::unique_ptr<rocksdb::Iterator> it(pimpl->db->NewIterator(options));
	rval = 0;

	for ( it->Seek("a"); it->Valid() && it->key()[0] == 'a'; it->Next() )
		++rval;

	if ( pimpl->require_ok(it->status()) )
		return rval;

	return {};
	}

broker::util::optional<broker::store::snapshot>
broker::store::rocksdb_backend::do_snap() const
	{
	if ( ! pimpl->require_db() )
		return {};

	rocksdb::ReadOptions options;
	options.fill_cache = false;
	std::unique_ptr<rocksdb::Iterator> it(pimpl->db->NewIterator(options));
	snapshot rval;
	rval.sn = pimpl->sn;

	std::unordered_map<data, expiration_time> expiries;

	for ( it->Seek("e"); it->Valid() && it->key()[0] == 'e'; it->Next() )
		{
		auto ks = it->key();
		auto vs = it->value();
		ks.remove_prefix(1);
		auto key = from_serial<data>(ks);
		expiries[std::move(key)] = from_serial<expiration_time>(vs);
		}

	if ( ! pimpl->require_ok(it->status()) )
		return {};

	for ( it->Seek("a"); it->Valid() && it->key()[0] == 'a'; it->Next() )
		{
		auto ks = it->key();
		auto vs = it->value();
		ks.remove_prefix(1);
		auto entry = std::make_pair(from_serial<data>(ks),
		                            value{from_serial<data>(vs)});
		auto eit = expiries.find(entry.first);

		if ( eit != expiries.end() )
			entry.second.expiry = std::move(eit->second);

		rval.entries.emplace_back(std::move(entry));
		}

	if ( ! pimpl->require_ok(it->status()) )
		return {};

	return rval;
	}

broker::util::optional<std::deque<broker::store::expirable>>
broker::store::rocksdb_backend::do_expiries() const
	{
	if ( ! pimpl->require_db() )
		return {};

	rocksdb::ReadOptions options;
	options.fill_cache = false;
	std::unique_ptr<rocksdb::Iterator> it(pimpl->db->NewIterator(options));
	std::deque<expirable> rval;

	for ( it->Seek("e"); it->Valid() && it->key()[0] == 'e'; it->Next() )
		{
		auto ks = it->key();
		auto vs = it->value();
		ks.remove_prefix(1);
		auto key = from_serial<data>(ks);
		auto expiry = from_serial<expiration_time>(vs);
		rval.emplace_back(expirable{std::move(key), std::move(expiry)});
		}

	if ( ! pimpl->require_ok(it->status()) )
		return {};

	return rval;
	}

bool broker::store::rocksdb_merge_operator::FullMerge(
        const rocksdb::Slice& key,
        const rocksdb::Slice* existing_value,
        const std::deque<std::string>& operand_list,
        std::string* new_value,
        rocksdb::Logger* logger) const
	{
	static std::string error;
	util::optional<data> new_data;

	if ( existing_value )
		new_data = from_serial<data>(*existing_value);

	for ( const auto& op : operand_list )
		{
		if ( op.empty() ) continue;

		switch ( op[0] ) {
		case '+':
			{
			auto by = from_serial<int64_t>(op.data() + 1, op.size() - 1);

			if ( ! util::increment_data(new_data, by, &error) )
				rocksdb::Log(logger, "%s", error.data());
			}
			break;
		case 'a':
			{
			auto element = from_serial<data>(op.data() + 1, op.size() - 1);

			if ( ! util::add_data_to_set(new_data, std::move(element), &error) )
				rocksdb::Log(logger, "%s", error.data());
			}
			break;
		case 'r':
			{
			auto element = from_serial<data>(op.data() + 1, op.size() - 1);

			if ( ! util::remove_data_from_set(new_data, element, &error) )
				rocksdb::Log(logger, "%s", error.data());
			}
			break;
		case 'h':
			{
			auto d = from_serial<data>(op.data() + 1, op.size() - 1);
			auto items = *get<vector>(d);

			if ( ! util::push_left(new_data, std::move(items), &error) )
				rocksdb::Log(logger, "%s", error.data());
			}
			break;
		case 't':
			{
			auto d = from_serial<data>(op.data() + 1, op.size() - 1);
			auto items = *get<vector>(d);

			if ( ! util::push_right(new_data, std::move(items), &error) )
				rocksdb::Log(logger, "%s", error.data());
			}
			break;
		default:
			rocksdb::Log(logger, "invalid operand: %d", op[0]);
			break;
		}
		}

	if ( new_data )
		new_value->assign(to_serial(*new_data));

	return true;
	}

bool broker::store::rocksdb_merge_operator::PartialMerge(
        const rocksdb::Slice& key,
        const rocksdb::Slice& left_operand,
        const rocksdb::Slice& right_operand,
        std::string* new_value,
        rocksdb::Logger* logger) const
	{
	auto lop = left_operand[0];
	auto rop = right_operand[0];
	rocksdb::Slice lslice(left_operand.data() + 1, left_operand.size() - 1);
	rocksdb::Slice rslice(right_operand.data() + 1, right_operand.size() - 1);

	if ( lop == '+' || rop == '+' )
		{
		if ( lop != rop )
			return false;

		auto lv = from_serial<int64_t>(lslice);
		auto rv = from_serial<int64_t>(rslice);
		auto nv = lv + rv;
		new_value->assign(to_serial(nv, '+'));
		return true;
		}

	if ( lop == 'h' )
		{
		if ( rop != 'h' )
			return false;

		auto ld = from_serial<data>(lslice);
		auto rd = from_serial<data>(rslice);
		auto lv = *get<vector>(ld);
		auto rv = *get<vector>(rd);

		for ( auto& e : lv )
			rv.emplace_back(std::move(e));

		new_value->assign(to_serial(rv, 'h'));
		}
	else if ( lop == 't' )
		{
		if ( rop != 't' )
			return false;

		auto ld = from_serial<data>(lslice);
		auto rd = from_serial<data>(rslice);
		auto lv = *get<vector>(ld);
		auto rv = *get<vector>(rd);

		for ( auto& e : rv )
			lv.emplace_back(std::move(e));

		new_value->assign(to_serial(lv, 't'));
		}

	if ( lslice != rslice )
		return false;

	if ( lop == 'a' )
		{
		if ( rop == 'a' )
			{
			new_value->assign(left_operand.data(), left_operand.size());
			return true;
			}
		else if ( rop == 'r' )
			{
			new_value->assign("");
			return true;
			}
		else
			return false;
		}
	else if ( lop == 'r' )
		{
		if ( rop == 'a' )
			{
			new_value->assign(right_operand.data(), right_operand.size());
			return true;
			}
		else if ( rop == 'r' )
			{
			new_value->assign(left_operand.data(), left_operand.size());
			return true;
			}
		else
			return false;
		}

	rocksdb::Log(logger, "invalid merge operand(s)");
	return false;
	}

const char* broker::store::rocksdb_merge_operator::Name() const
	{ return "broker_merge_op"; }
