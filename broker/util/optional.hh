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
#include <functional>
#include <broker/util/none.hh>
#include <broker/util/operators.hh>

namespace broker {
namespace util {

/**
 * Represents an optional value of `T`.
 */
template <class T>
class optional : totally_ordered<optional<T>>,
                 totally_ordered<optional<T>, none_type>,
                 totally_ordered<none_type, optional<T>>,
                 totally_ordered<optional<T>, T>,
                 totally_ordered<T, optional<T>>,
                 totally_ordered<optional<T&>, T>,
                 totally_ordered<T, optional<T&>>,
                 totally_ordered<optional<const T&>, T>,
                 totally_ordered<T, optional<const T&>>
{
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
	  * Creates an valid instance from `arg_value`.
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

// Relational operators
template <typename T>
inline bool operator==(const optional<T>& x, const optional<T>& y)
	{ return bool(x) != bool(y) ? false : bool(x) == false ? true : *x == *y; }

template <typename T>
inline bool operator<(const optional<T>& x, const optional<T>& y)
	{ return (!y) ? false : (!x) ? true : *x < *y; }

// Comparison with none
template <class T>
inline bool operator==(const optional<T>& x, const none_type&) noexcept
	{ return (!x); }
template <class T>
inline bool operator==(const none_type&, const optional<T>& x) noexcept
	{ return (!x); }

template <class T>
inline bool operator<(const optional<T>&, const none_type&) noexcept
	{ return false; }
template <class T>
inline bool operator<(const none_type&, const optional<T>& x) noexcept
	{ return bool(x); }

// Comparison with T
template <class T>
inline bool operator==(const optional<T>& x, const T& v)
	{ return bool(x) ? *x == v : false; }
template <class T>
inline bool operator==(const T& v, const optional<T>& x)
	{ return bool(x) ? v == *x : false; }

template <class T>
inline bool operator<(const optional<T>& x, const T& v)
	{ return bool(x) ? *x < v : true; }
template <class T>
inline bool operator<(const T& v, const optional<T>& x)
	{ return bool(x) ? v < *x : false; }

// Comparison of optional<T&> with T
template <class T>
inline bool operator==(const optional<T&>& x, const T& v)
	{ return bool(x) ? *x == v : false; }
template <class T>
inline bool operator==(const T& v, const optional<T&>& x)
	{ return bool(x) ? v == *x : false; }

template <class T>
inline bool operator<(const optional<T&>& x, const T& v)
	{ return bool(x) ? *x < v : true; }
template <class T>
inline bool operator<(const T& v, const optional<T&>& x)
	{ return bool(x) ? v < *x : false; }

// Comparison of optional<const T&> with T
template <class T>
inline bool operator==(const optional<const T&>& x, const T& v)
	{ return bool(x) ? *x == v : false; }
template <class T>
inline bool operator==(const T& v, const optional<const T&>& x)
	{ return bool(x) ? v == *x : false; }

template <class T>
inline bool operator<(const optional<const T&>& x, const T& v)
	{ return bool(x) ? *x < v : true; }
template <class T>
inline bool operator<(const T& v, const optional<const T&>& x)
	{ return bool(x) ? v < *x : false; }

} // namespace util
} // namespace broker

namespace std {
template <typename T>
struct hash<broker::util::optional<T>> {
	using result_type = typename hash<T>::result_type;
	using argument_type = broker::util::optional<T>;

	inline result_type operator()(const argument_type& arg) const
		{
		if ( arg ) return std::hash<T>{}(*arg);
		return result_type{};
		}
};

template <typename T>
struct hash<broker::util::optional<T&>> {
	using result_type = typename hash<T>::result_type;
	using argument_type = broker::util::optional<T&>;

	inline result_type operator()(const argument_type& arg) const
		{
		if ( arg ) return std::hash<T>{}(*arg);
		return result_type{};
		}
};
}

#endif // BROKER_UTIL_OPTIONAL_HH
