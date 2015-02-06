#include "broker/store/backend.hh"

broker::store::backend::~backend() = default;

std::string broker::store::backend::last_error() const
	{ return do_last_error(); }

bool broker::store::backend::init(snapshot sss)
	{ return do_init(std::move(sss)); }

const broker::store::sequence_num& broker::store::backend::sequence() const
	{ return do_sequence(); }

bool broker::store::backend::insert(data k, data v,
                                    util::optional<expiration_time> t)
	{
	if ( ! do_insert(std::move(k), std::move(v), std::move(t)) )
		return false;

	do_increase_sequence();
	return true;
	}

broker::store::modification_result
broker::store::backend::increment(const data& k, int64_t by, double mod_time)
	{
	auto rc = do_increment(k, by, mod_time);

	if ( rc.stat == broker::store::modification_result::status::success )
		do_increase_sequence();

	return rc;
	}

broker::store::modification_result
broker::store::backend::add_to_set(const data& k, data element, double mod_time)
	{
	auto rc = do_add_to_set(k, std::move(element), mod_time);

	if ( rc.stat == broker::store::modification_result::status::success )
		do_increase_sequence();

	return rc;
	}

broker::store::modification_result
broker::store::backend::remove_from_set(const data& k, const data& element,
                                        double mod_time)
	{
	auto rc = do_remove_from_set(k, element, mod_time);

	if ( rc.stat == broker::store::modification_result::status::success )
		do_increase_sequence();

	return rc;
	}

bool broker::store::backend::erase(const data& k)
	{
	if ( ! do_erase(k) )
		return false;

	do_increase_sequence();
	return true;
	}

bool broker::store::backend::expire(const data& k,
                                    const expiration_time& expiration)
	{
	if ( ! do_expire(k, expiration) )
		return false;

	do_increase_sequence();
	return true;
	}

bool broker::store::backend::clear()
	{
	if ( ! do_clear() )
		return false;

	do_increase_sequence();
	return true;
	}

broker::store::modification_result
broker::store::backend::push_left(const data& k, vector items, double mod_time)
	{
	auto rc = do_push_left(k, std::move(items), mod_time);

	if ( rc.stat == broker::store::modification_result::status::success )
		do_increase_sequence();

	return rc;
	}

broker::store::modification_result
broker::store::backend::push_right(const data& k, vector items, double mod_time)
	{
	auto rc = do_push_right(k, std::move(items), mod_time);

	if ( rc.stat == broker::store::modification_result::status::success )
		do_increase_sequence();

	return rc;
	}

std::pair<broker::store::modification_result,
          broker::util::optional<broker::data>>
broker::store::backend::pop_left(const data& k, double mod_time)
	{
	auto rc = do_pop_left(k, mod_time);

	if ( rc.first.stat == modification_result::status::success && rc.second )
		do_increase_sequence();

	return rc;
	}

std::pair<broker::store::modification_result,
          broker::util::optional<broker::data>>
broker::store::backend::pop_right(const data& k, double mod_time)
	{
	auto rc = do_pop_right(k, mod_time);

	if ( rc.first.stat == modification_result::status::success && rc.second )
		do_increase_sequence();

	return rc;
	}

broker::util::optional<broker::util::optional<broker::data>>
broker::store::backend::lookup(const data& k) const
	{ return do_lookup(k); }

broker::util::optional<bool> broker::store::backend::exists(const data& k) const
	{ return do_exists(k); }

broker::util::optional<std::vector<broker::data>>
broker::store::backend::keys() const
	{ return do_keys(); }

broker::util::optional<uint64_t> broker::store::backend::size() const
	{ return do_size(); }

broker::util::optional<broker::store::snapshot> broker::store::backend::snap() const
	{ return do_snap(); }

broker::util::optional<std::deque<broker::store::expirable>>
broker::store::backend::expiries() const
	{ return do_expiries(); }
