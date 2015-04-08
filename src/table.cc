// Begin C API
#include "broker/broker.h"
#include "broker/data.hh"
using std::nothrow;

broker_table* broker_table_create()
	{ return reinterpret_cast<broker_table*>(new (nothrow) broker::table()); }

void broker_table_delete(broker_table* t)
	{ delete reinterpret_cast<broker::table*>(t); }

broker_table* broker_table_copy(const broker_table* t)
	{
	auto tt = reinterpret_cast<const broker::table*>(t);

	try
		{ return reinterpret_cast<broker_table*>(new broker::table(*tt)); }
	catch ( std::bad_alloc& )
		{ return nullptr; }
	}

void broker_table_clear(broker_table* t)
	{
	auto tt = reinterpret_cast<broker::table*>(t);
	tt->clear();
	}

size_t broker_table_size(const broker_table* t)
	{
	auto tt = reinterpret_cast<const broker::table*>(t);
	return tt->size();
	}

int broker_table_contains(const broker_table* t, const broker_data* key)
	{
	auto tt = reinterpret_cast<const broker::table*>(t);
	auto kk = reinterpret_cast<const broker::data*>(key);

	if ( tt->find(*kk) == tt->end() )
		return 0;

	return 1;
	}

int broker_table_insert(broker_table* t, const broker_data* key,
                        const broker_data* value)
	{
	auto tt = reinterpret_cast<broker::table*>(t);
	auto kk = reinterpret_cast<const broker::data*>(key);
	auto vv = reinterpret_cast<const broker::data*>(value);

	try
		{ return tt->emplace(*kk, *vv).second; }
	catch ( std::bad_alloc& )
		{ return -1; }
	}

int broker_table_remove(broker_table* t, const broker_data* key)
	{
	auto tt = reinterpret_cast<broker::table*>(t);
	auto kk = reinterpret_cast<const broker::data*>(key);
	return tt->erase(*kk);
	}

broker_data* broker_table_lookup(broker_table* t, const broker_data* key)
	{
	auto tt = reinterpret_cast<broker::table*>(t);
	auto kk = reinterpret_cast<const broker::data*>(key);
	auto it = tt->find(*kk);

	if ( it == tt->end() )
		return 0;

	broker::data& rval = it->second;
	return reinterpret_cast<broker_data*>(&rval);
	}

int broker_table_eq(const broker_table* a, const broker_table* b)
	{
	auto aa = reinterpret_cast<const broker::table*>(a);
	auto bb = reinterpret_cast<const broker::table*>(b);
	return *aa == *bb;
	}

int broker_table_lt(const broker_table* a, const broker_table* b)
	{
	auto aa = reinterpret_cast<const broker::table*>(a);
	auto bb = reinterpret_cast<const broker::table*>(b);
	return *aa < *bb;
	}

size_t broker_table_hash(const broker_table* a)
	{
	auto aa = reinterpret_cast<const broker::table*>(a);
	return std::hash<broker::table>{}(*aa);
	}

broker_table_iterator* broker_table_iterator_create(broker_table* t)
	{
	auto tt = reinterpret_cast<broker::table*>(t);
	auto rval = new (nothrow) broker::table::iterator(tt->begin());
	return reinterpret_cast<broker_table_iterator*>(rval);
	}

void broker_table_iterator_delete(broker_table_iterator* it)
	{ delete reinterpret_cast<broker::table::iterator*>(it); }

int broker_table_iterator_at_last(broker_table* t, broker_table_iterator* it)
	{
	auto tt = reinterpret_cast<broker::table*>(t);
	auto ii = reinterpret_cast<broker::table::iterator*>(it);
	return *ii == tt->end();
	}

int broker_table_iterator_next(broker_table* t, broker_table_iterator* it)
	{
	auto tt = reinterpret_cast<broker::table*>(t);
	auto ii = reinterpret_cast<broker::table::iterator*>(it);
	++(*ii);
	return *ii == tt->end();
	}

broker_table_entry broker_table_iterator_value(broker_table_iterator* it)
	{
	auto ii = reinterpret_cast<broker::table::iterator*>(it);
	auto k = reinterpret_cast<const broker_data*>(&(*ii)->first);
	auto v = reinterpret_cast<broker_data*>(&(*ii)->second);
	return {k, v};
	}

broker_table_const_iterator*
broker_table_const_iterator_create(const broker_table* t)
	{
	auto tt = reinterpret_cast<const broker::table*>(t);
	auto rval = new (nothrow) broker::table::const_iterator(tt->begin());
	return reinterpret_cast<broker_table_const_iterator*>(rval);
	}

void broker_table_const_iterator_delete(broker_table_const_iterator* it)
	{ delete reinterpret_cast<broker::table::const_iterator*>(it); }

int broker_table_const_iterator_at_last(const broker_table* t,
                                        broker_table_const_iterator* it)
	{
	auto tt = reinterpret_cast<const broker::table*>(t);
	auto ii = reinterpret_cast<broker::table::const_iterator*>(it);
	return *ii == tt->end();
	}

int broker_table_const_iterator_next(const broker_table* t,
                                     broker_table_const_iterator* it)
	{
	auto tt = reinterpret_cast<const broker::table*>(t);
	auto ii = reinterpret_cast<broker::table::const_iterator*>(it);
	++(*ii);
	return *ii == tt->end();
	}

broker_const_table_entry
broker_table_const_iterator_value(broker_table_const_iterator* it)
	{
	auto ii = reinterpret_cast<broker::table::const_iterator*>(it);
	auto k = reinterpret_cast<const broker_data*>(&(*ii)->first);
	auto v = reinterpret_cast<const broker_data*>(&(*ii)->second);
	return {k, v};
	}
