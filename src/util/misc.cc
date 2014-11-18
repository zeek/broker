#include "misc.hh"
#include <cstdio>

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
