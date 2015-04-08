// Begin C API
#include "broker/broker.h"
#include "broker/data.hh"
using std::nothrow;

broker_vector* broker_vector_create()
	{ return reinterpret_cast<broker_vector*>(new (nothrow) broker::vector()); }

void broker_vector_delete(broker_vector* v)
	{ delete reinterpret_cast<broker::vector*>(v); }

broker_vector* broker_vector_copy(const broker_vector* v)
	{
	auto vv = reinterpret_cast<const broker::vector*>(v);

	try
		{ return reinterpret_cast<broker_vector*>(new broker::vector(*vv)); }
	catch ( std::bad_alloc& )
		{ return nullptr; }
	}

void broker_vector_clear(broker_vector* v)
	{
	auto vv = reinterpret_cast<broker::vector*>(v);
	vv->clear();
	}

size_t broker_vector_size(const broker_vector* v)
	{
	auto vv = reinterpret_cast<const broker::vector*>(v);
	return vv->size();
	}

int broker_vector_reserve(broker_vector* v, size_t size)
	{
	auto vv = reinterpret_cast<broker::vector*>(v);

	try
		{
		vv->reserve(size);
		return 1;
		}
	catch ( std::bad_alloc& )
		{ return 0; }
	}

int broker_vector_insert(broker_vector* v, const broker_data* d, size_t idx)
	{
	auto vv = reinterpret_cast<broker::vector*>(v);
	auto dd = reinterpret_cast<const broker::data*>(d);

	try
		{
		vv->emplace(vv->begin() + idx, *dd);
		return 1;
		}
	catch ( std::bad_alloc& )
		{ return 0; }
	}

int broker_vector_replace(broker_vector* v, const broker_data* d, size_t idx)
	{
	auto vv = reinterpret_cast<broker::vector*>(v);
	auto dd = reinterpret_cast<const broker::data*>(d);

	try
		{
		(*vv)[idx] = *dd;
		return 1;
		}
	catch ( std::bad_alloc& )
		{ return 0; }
	}

int broker_vector_remove(broker_vector* v, size_t idx)
	{
	auto vv = reinterpret_cast<broker::vector*>(v);
	vv->erase(vv->begin() + idx);
	return 1;
	}

broker_data* broker_vector_lookup(broker_vector* v, size_t idx)
	{
	auto vv = reinterpret_cast<broker::vector*>(v);
	return reinterpret_cast<broker_data*>(&(*vv)[idx]);
	}

int broker_vector_eq(const broker_vector* a, const broker_vector* b)
	{
	auto aa = reinterpret_cast<const broker::vector*>(a);
	auto bb = reinterpret_cast<const broker::vector*>(b);
	return *aa == *bb;
	}

int broker_vector_lt(const broker_vector* a, const broker_vector* b)
	{
	auto aa = reinterpret_cast<const broker::vector*>(a);
	auto bb = reinterpret_cast<const broker::vector*>(b);
	return *aa < *bb;
	}

size_t broker_vector_hash(const broker_vector* a)
	{
	auto aa = reinterpret_cast<const broker::vector*>(a);
	return std::hash<broker::vector>{}(*aa);
	}

broker_vector_iterator* broker_vector_iterator_create(broker_vector* v)
	{
	auto vv = reinterpret_cast<broker::vector*>(v);
	auto rval = new (nothrow) broker::vector::iterator(vv->begin());
	return reinterpret_cast<broker_vector_iterator*>(rval);
	}

void broker_vector_iterator_delete(broker_vector_iterator* it)
	{ delete reinterpret_cast<broker::vector::iterator*>(it); }

int broker_vector_iterator_at_last(broker_vector* v,
                                   broker_vector_iterator* it)
	{
	auto vv = reinterpret_cast<broker::vector*>(v);
	auto ii = reinterpret_cast<broker::vector::iterator*>(it);
	return *ii == vv->end();
	}

int broker_vector_iterator_next(broker_vector* v, broker_vector_iterator* it)
	{
	auto vv = reinterpret_cast<broker::vector*>(v);
	auto ii = reinterpret_cast<broker::vector::iterator*>(it);
	++(*ii);
	return *ii == vv->end();
	}

broker_data* broker_vector_iterator_value(broker_vector_iterator* it)
	{
	auto ii = reinterpret_cast<broker::vector::iterator*>(it);
	return reinterpret_cast<broker_data*>(&**ii);
	}

broker_vector_const_iterator*
broker_vector_const_iterator_create(const broker_vector* v)
	{
	auto vv = reinterpret_cast<const broker::vector*>(v);
	auto rval = new (nothrow) broker::vector::const_iterator(vv->begin());
	return reinterpret_cast<broker_vector_const_iterator*>(rval);
	}

void broker_vector_const_iterator_delete(broker_vector_const_iterator* it)
	{ delete reinterpret_cast<broker::vector::const_iterator*>(it); }

int broker_vector_const_iterator_at_last(const broker_vector* v,
                                   broker_vector_const_iterator* it)
	{
	auto vv = reinterpret_cast<const broker::vector*>(v);
	auto ii = reinterpret_cast<broker::vector::const_iterator*>(it);
	return *ii == vv->end();
	}

int broker_vector_const_iterator_next(const broker_vector* v,
                                      broker_vector_const_iterator* it)
	{
	auto vv = reinterpret_cast<const broker::vector*>(v);
	auto ii = reinterpret_cast<broker::vector::const_iterator*>(it);
	++(*ii);
	return *ii == vv->end();
	}

const broker_data*
broker_vector_const_iterator_value(broker_vector_const_iterator* it)
	{
	auto ii = reinterpret_cast<broker::vector::const_iterator*>(it);
	return reinterpret_cast<const broker_data*>(&**ii);
	}
