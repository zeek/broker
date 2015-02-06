#include "misc.hh"
#include <cstdio>

broker::util::optional<broker::store::expiration_time>
broker::util::update_last_modification(optional<store::expiration_time>& et,
                                       double mod_time)
	{
	if ( ! et )
		return {};

	if ( et->type == store::expiration_time::tag::absolute )
		return {};

	et->modification_time = mod_time;
	return et;
	}

bool broker::util::increment_data(optional<data>& d, int64_t by,
                                  std::string* error_msg)
	{
	if ( d )
		return increment_data(*d, by, error_msg);

	d = data{by};
	return true;
	}

bool broker::util::increment_data(data& d, int64_t by, std::string* error_msg)
	{
	if ( visit(increment_visitor{by}, d) )
		return true;

	if ( error_msg )
		{
		char tmp[64];
		snprintf(tmp, sizeof(tmp), "attempt to increment non-integral tag %d",
		         static_cast<int>(which(d)));
		*error_msg = tmp;
		}

	return false;
	}

bool broker::util::add_data_to_set(optional<data>& s, data element,
                                   std::string* error_msg)
	{
	if ( s )
		return add_data_to_set(*s, std::move(element), error_msg);

	s = data{broker::set{std::move(element)}};
	return true;
	}

bool broker::util::add_data_to_set(data& s, data element,
                                   std::string* error_msg)
	{
	broker::set* v = get<broker::set>(s);

	if ( v )
		{
		v->emplace(std::move(element));
		return true;
		}

	if ( error_msg )
		{
		char tmp[64];
		snprintf(tmp, sizeof(tmp), "attempt to add to non-set tag %d",
		         static_cast<int>(which(s)));
		*error_msg = tmp;
		}

	return false;
	}

bool broker::util::remove_data_from_set(optional<data>& s, const data& element,
                                        std::string* error_msg)
	{
	if ( s )
		return remove_data_from_set(*s, element, error_msg);

	s = data{broker::set{}};
	return true;
	}

bool broker::util::remove_data_from_set(data& s, const data& element,
                                        std::string* error_msg)
	{
	broker::set* v = get<broker::set>(s);

	if ( v )
		{
		v->erase(element);
		return true;
		}

	if ( error_msg )
		{
		char tmp[64];
		snprintf(tmp, sizeof(tmp), "attempt to remove from non-set tag %d",
		         static_cast<int>(which(s)));
		*error_msg = tmp;
		}

	return false;
	}

bool broker::util::push_left(optional<data>& v, vector items,
                             std::string* error_msg)
	{
	if ( v )
		return push_left(*v, std::move(items), error_msg);

	v = data{std::move(items)};
	return true;
	}

bool broker::util::push_left(data& v, vector items, std::string* error_msg)
	{
	broker::vector* vv = get<broker::vector>(v);

	if ( vv )
		{
		items.reserve(items.size() + vv->size());

		for ( auto& i : *vv )
			items.emplace_back(std::move(i));

		using std::swap;
		swap(items, *vv);
		return true;
		}

	if ( error_msg )
		{
		char tmp[64];
		snprintf(tmp, sizeof(tmp), "attempt to push to a non-vector tag %d",
		         static_cast<int>(which(v)));
		*error_msg = tmp;
		}

	return false;
	}

bool broker::util::push_right(optional<data>& v, vector items,
                              std::string* error_msg)
	{
	if ( v )
		return push_right(*v, std::move(items), error_msg);

	v = data{std::move(items)};
	return true;
	}

bool broker::util::push_right(data& v, vector items, std::string* error_msg)
	{
	broker::vector* vv = get<broker::vector>(v);

	if ( vv )
		{
		vv->reserve(items.size() + vv->size());

		for ( auto& i : items )
			vv->emplace_back(std::move(i));

		return true;
		}

	if ( error_msg )
		{
		char tmp[64];
		snprintf(tmp, sizeof(tmp), "attempt to push to a non-vector tag %d",
		         static_cast<int>(which(v)));
		*error_msg = tmp;
		}

	return false;
	}

broker::util::optional<broker::util::optional<broker::data>>
broker::util::pop_left(broker::util::optional<broker::data>& v,
                       std::string* error_msg, bool shrink)
	{
	if ( v )
		return pop_left(*v, error_msg, shrink);

	return {optional<data>{}};
	}

broker::util::optional<broker::util::optional<broker::data>>
broker::util::pop_left(broker::data& v, std::string* error_msg, bool shrink)
	{
	broker::vector* vv = get<broker::vector>(v);

	if ( vv )
		{
		if ( vv->empty() )
			return {optional<data>{}};

		auto rval = std::move(vv->front());
		// NIT: removing front of std::vector is slow, but can't do much else
		// other than change broker::vector to use std::deque.  That is worth
		// consideration; the only thing stopping that at the moment is
		// std::deque<T> won't work for incomplete type T's (broker::data in
		// this case).  Technically, all STL containers claim undefined
		// behavior for incomplete types, but compilers still work for some
		// (e.g. vector, set, map).
		vv->erase(vv->begin());

		if ( shrink && vv->capacity() > vv->size() * 8 )
			vv->shrink_to_fit();

		return {std::move(rval)};
		}

	if ( error_msg )
		{
		char tmp[64];
		snprintf(tmp, sizeof(tmp), "attempt to pop from a non-vector tag %d",
		         static_cast<int>(which(v)));
		*error_msg = tmp;
		}

	return {};
	}

broker::util::optional<broker::util::optional<broker::data>>
broker::util::pop_right(broker::util::optional<broker::data>& v,
                        std::string* error_msg, bool shrink)
	{
	if ( v )
		return pop_right(*v, error_msg, shrink);

	return {optional<data>{}};
	}

broker::util::optional<broker::util::optional<broker::data>>
broker::util::pop_right(broker::data& v, std::string* error_msg, bool shrink)
	{
	broker::vector* vv = get<broker::vector>(v);

	if ( vv )
		{
		if ( vv->empty() )
			return {optional<data>{}};

		auto rval = std::move(vv->back());
		vv->pop_back();

		if ( shrink && vv->capacity() > vv->size() * 8 )
			vv->shrink_to_fit();

		vv->shrink_to_fit();
		return {std::move(rval)};
		}

	if ( error_msg )
		{
		char tmp[64];
		snprintf(tmp, sizeof(tmp), "attempt to pop from a non-vector tag %d",
		         static_cast<int>(which(v)));
		*error_msg = tmp;
		}

	return {};
	}
