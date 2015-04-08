// Begin C API
#include "broker/broker.h"
#include "broker/data.hh"
using std::nothrow;

broker_record* broker_record_create(size_t size)
	{
	try
		{
		auto fields = std::vector<broker::record::field>(size);
		auto rval = new broker::record(std::move(fields));
		return reinterpret_cast<broker_record*>(rval);
		}
	catch ( std::bad_alloc& )
		{ return nullptr; }
	}

void broker_record_delete(broker_record* r)
	{ delete reinterpret_cast<broker::record*>(r); }

broker_record* broker_record_copy(const broker_record* r)
	{
	auto rr = reinterpret_cast<const broker::record*>(r);

	try
		{ return reinterpret_cast<broker_record*>(new broker::record(*rr)); }
	catch ( std::bad_alloc& )
		{ return nullptr; }
	}

size_t broker_record_size(const broker_record* r)
	{
	auto rr = reinterpret_cast<const broker::record*>(r);
	return rr->size();
	}

int broker_record_assign(broker_record* r, const broker_data* d, size_t idx)
	{
	auto rr = reinterpret_cast<broker::record*>(r);
	auto dd = reinterpret_cast<const broker::data*>(d);

	try
		{ rr->fields[idx] = *dd; }
	catch ( std::bad_alloc& )
		{ return 0; }

	return 1;
	}

broker_data* broker_record_lookup(broker_record* r, size_t idx)
	{
	auto rr = reinterpret_cast<broker::record*>(r);
	auto opt_data = rr->get(idx);

	if ( opt_data )
		return reinterpret_cast<broker_data*>(&*opt_data);

	return 0;
	}

int broker_record_eq(const broker_record* a, const broker_record* b)
	{
	auto aa = reinterpret_cast<const broker::record*>(a);
	auto bb = reinterpret_cast<const broker::record*>(b);
	return *aa == *bb;
	}

int broker_record_lt(const broker_record* a, const broker_record* b)
	{
	auto aa = reinterpret_cast<const broker::record*>(a);
	auto bb = reinterpret_cast<const broker::record*>(b);
	return *aa < *bb;
	}

size_t broker_record_hash(const broker_record* a)
	{
	auto aa = reinterpret_cast<const broker::record*>(a);
	return std::hash<broker::record>{}(*aa);
	}

using record_iterator = std::vector<broker::record::field>::iterator;
using record_const_iterator = std::vector<broker::record::field>::const_iterator;

broker_record_iterator* broker_record_iterator_create(broker_record* r)
	{
	auto rr = reinterpret_cast<broker::record*>(r);
	auto rval = new (nothrow) record_iterator(rr->fields.begin());
	return reinterpret_cast<broker_record_iterator*>(rval);
	}

void broker_record_iterator_delete(broker_record_iterator* it)
	{ delete reinterpret_cast<record_iterator*>(it); }

int broker_record_iterator_at_last(broker_record* r, broker_record_iterator* it)
	{
	auto rr = reinterpret_cast<broker::record*>(r);
	auto ii = reinterpret_cast<record_iterator*>(it);
	return *ii == rr->fields.end();
	}

int broker_record_iterator_next(broker_record* r, broker_record_iterator* it)
	{
	auto rr = reinterpret_cast<broker::record*>(r);
	auto ii = reinterpret_cast<record_iterator*>(it);
	++(*ii);
	return *ii == rr->fields.end();
	}

broker_data* broker_record_iterator_value(broker_record_iterator* it)
	{
	auto ii = reinterpret_cast<record_iterator*>(it);
	broker::record::field& f = **ii;

	if ( f )
		return reinterpret_cast<broker_data*>(&*f);

	return 0;
	}

broker_record_const_iterator*
broker_record_const_iterator_create(const broker_record* r)
	{
	auto rr = reinterpret_cast<const broker::record*>(r);
	auto rval = new (nothrow) record_const_iterator(rr->fields.begin());
	return reinterpret_cast<broker_record_const_iterator*>(rval);
	}

void broker_record_const_iterator_delete(broker_record_const_iterator* it)
	{ delete reinterpret_cast<record_const_iterator*>(it); }

int broker_record_const_iterator_at_last(const broker_record* r,
                                         broker_record_const_iterator* it)
	{
	auto rr = reinterpret_cast<const broker::record*>(r);
	auto ii = reinterpret_cast<record_const_iterator*>(it);
	return *ii == rr->fields.end();
	}

int broker_record_const_iterator_next(const broker_record* r,
                                      broker_record_const_iterator* it)
	{
	auto rr = reinterpret_cast<const broker::record*>(r);
	auto ii = reinterpret_cast<record_const_iterator*>(it);
	++(*ii);
	return *ii == rr->fields.end();
	}

const broker_data*
broker_record_const_iterator_value(broker_record_const_iterator* it)
	{
	auto ii = reinterpret_cast<record_const_iterator*>(it);
	const broker::record::field& f = **ii;

	if ( f )
		return reinterpret_cast<const broker_data*>(&*f);

	return 0;
	}
