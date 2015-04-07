#include "broker/store/query.hh"
// Begin C API
#include "broker/broker.h"
using std::nothrow;

broker_store_query* broker_store_query_create(broker_store_query_tag tag,
                                              const broker_data* key)
	{
	auto tt = static_cast<broker::store::query::tag>(tag);
	auto kk = reinterpret_cast<const broker::data*>(key);

	try
		{
		broker::data k;

		if ( kk )
			k = *kk;

		auto rval = new broker::store::query(tt, std::move(k));
		return reinterpret_cast<broker_store_query*>(rval);
		}
	catch ( std::bad_alloc& )
		{}

	return nullptr;
	}

void broker_store_query_delete(broker_store_query* q)
	{
	delete reinterpret_cast<broker::store::query*>(q);
	}

broker_store_query_tag broker_store_query_get_tag(const broker_store_query* q)
	{
	auto qq = reinterpret_cast<const broker::store::query*>(q);
	return static_cast<broker_store_query_tag>(qq->type);
	}

broker_data* broker_store_query_key(broker_store_query* q)
	{
	auto qq = reinterpret_cast<broker::store::query*>(q);
	return reinterpret_cast<broker_data*>(&qq->k);
	}
