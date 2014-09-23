/*
Copyright (c) 2011-2014, Dominik Charousset
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

    * Redistributions of source code must retain the above copyright
      notice, this list of conditions and the following disclaimer.

    * Redistributions in binary form must reproduce the above copyright
      notice, this list of conditions and the following disclaimer in the
      documentation and/or other materials provided with the distribution.

    * Neither the name of the copyright holder nor the names of its
      contributors may be used to endorse or promote products derived from
      this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDERS BE LIABLE FOR ANY DIRECT,
INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE
OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

#ifndef BROKER_UTIL_OPTIONAL_HH
#define BROKER_UTIL_OPTIONAL_HH

#include <new>
#include <utility>
#include <cassert>
#include <broker/util/none.hh>

namespace broker {
namespace util {

/**
 * Represents an optional value of `T`.
 */
template <class T>
class optional {
public:

	/**
	  * Alias for `T`.
	  */
	using type = T;

	/**
	  * Creates an empty instance with `valid() == false`.
	  */
	optional(const none_type& = none)
		: is_valid(false)
		{ }

	/**
	  * Creates an valid instance from `value`.
	  */
	optional(T arg_value)
		: is_valid(false)
		{ create(std::move(arg_value)); }

	/**
	 * Create an optional instance by copying another.
	 */
	optional(const optional& other)
		: is_valid(false)
		{ if ( other.is_valid ) create(other.value); }

	/**
	 * Create an optional instance by stealing another.
	 */
	optional(optional&& other)
		: is_valid(false)
		{ if ( other.is_valid ) create(std::move(other.value)); }

	/**
	  * Destructor.
	  */
	~optional()
		{ destroy(); }

	/**
	 * Assign to an optional instance by copying another.
	 */
	optional& operator=(const optional& other)
		{
		if ( is_valid )
			{
			if ( other.is_valid )
				value = other.value;
			else
				destroy();
			}
		else if ( other.is_valid )
			create(other.value);
		return *this;
		}

	/**
	 * Assign to an optional instance by stealing another.
	 */
	optional& operator=(optional&& other)
		{
		if ( is_valid )
			{
			if ( other.is_valid )
				value = std::move(other.value);
			else
				destroy();
			}
		else if (other.is_valid)
			create(std::move(other.value));
		return *this;
		}

	/**
	  * Queries whether this instance holds a value.
	  */
	bool valid() const
		{ return is_valid; }

	/**
	  * Returns `!valid()`.
	  */
	bool empty() const
		{ return ! is_valid; }

	/**
	  * Returns `valid()`.
	  */
	explicit operator bool() const
		{ return valid(); }

	/**
	  * Returns `!valid()`.
	  */
	bool operator!() const
		{ return ! valid(); }

	/**
	  * Returns the value.
	  */
	T& operator*()
		{
		assert(valid());
		return value;
		}

	/**
	  * Returns the value.
	  */
	const T& operator*() const
		{
		assert(valid());
		return value;
		}

	/**
	  * Returns the value.
	  */
	const T* operator->() const
		{
		assert(valid());
		return &value;
		}

	/**
	  * Returns the value.
	  */
	T* operator->()
		{
		assert(valid());
		return &value;
		}

	/**
	  * Returns the value.
	  */
	T& get()
		{
		assert(valid());
		return value;
		}

	/**
	  * Returns the value.
	  */
	const T& get() const
		{
		assert(valid());
		return value;
		}

	/**
	  * Returns the value if `valid()`, otherwise returns `default_value`.
	  */
	const T& get_or_else(const T& default_value) const
		{
		if ( valid() )
			return get();
		return default_value;
		}

private:

	void destroy()
		{
		if ( is_valid )
			{
			value.~T();
			is_valid = false;
			}
		}

	template <class V>
	void create(V&& arg_value)
		{
		assert(! valid());
		is_valid = true;
		new (&value) T(std::forward<V>(arg_value));
		}

	bool is_valid;
	union { T value; };
};

/**
 * Template specialization to allow `optional`
 * to hold a reference rather than an actual value.
 */
template <class T>
class optional<T&> {
public:

	using type = T;

	optional(const none_type& = none)
		: value(nullptr)
		{ }

	optional(T& arg_value)
		: value(&arg_value)
		{ }

	optional(const optional& other) = default;

	optional& operator=(const optional& other) = default;

	bool valid() const
		{ return value != nullptr; }

	bool empty() const
		{ return ! valid(); }

	explicit operator bool() const
		{ return valid(); }

	bool operator!() const
		{ return ! valid(); }

	T& operator*()
		{
		assert(valid());
		return *value;
		}

	const T& operator*() const
		{
		assert(valid());
		return *value;
		}

	T* operator->()
		{
		assert(valid());
		return value;
		}

	const T* operator->() const
		{
		assert(valid());
		return value;
		}

	T& get()
		{
		assert(valid());
		return *value;
		}

	const T& get() const
		{
		assert(valid());
		return *value;
		}

	const T& get_or_else(const T& default_value) const
		{
		if ( valid() )
			return get();
		return default_value;
		}

private:

	T* value;
};

/** @relates optional */
template <class T, typename U>
bool operator==(const optional<T>& lhs, const optional<U>& rhs)
	{
	if ( (lhs) && (rhs) ) return *lhs == *rhs;
	return false;
	}

/** @relates optional */
template <class T, typename U>
bool operator==(const optional<T>& lhs, const U& rhs)
	{
	if ( lhs ) return *lhs == rhs;
	return false;
	}

/** @relates optional */
template <class T, typename U>
bool operator==(const T& lhs, const optional<U>& rhs)
	{ return rhs == lhs; }

/** @relates optional */
template <class T, typename U>
bool operator!=(const optional<T>& lhs, const optional<U>& rhs)
	{ return ! (lhs == rhs); }

/** @relates optional */
template <class T, typename U>
bool operator!=(const optional<T>& lhs, const U& rhs)
	{ return ! (lhs == rhs); }

/** @relates optional */
template <class T, typename U>
bool operator!=(const T& lhs, const optional<U>& rhs)
	{ return ! (lhs == rhs); }

/** @relates optional */
template <class T>
bool operator==(const optional<T>& val, const none_type&)
	{ return val.valid(); }

/** @relates optional */
template <class T>
bool operator==(const none_type&, const optional<T>& val)
	{ return val.valid(); }

/** @relates optional */
template <class T>
bool operator!=(const optional<T>& val, const none_type&)
	{ return ! val.valid(); }

/** @relates optional */
template <class T>
bool operator!=(const none_type&, const optional<T>& val)
	{ return ! val.valid(); }

} // namespace util
} // namespace broker

#endif // BROKER_UTIL_OPTIONAL_HH
