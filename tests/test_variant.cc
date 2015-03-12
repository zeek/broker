#include "broker/util/variant.hh"
#include "testsuite.h"
#include <string>
#include <vector>
#include <algorithm>

using namespace std;
using namespace broker::util;

enum class tt : int {
	zero = 0,
	one = 1,
	two = 2
};

using myvariant = default_variant<int, string>;
using recursive = default_variant<myvariant, vector<myvariant>>;

struct size_visitor {
	using result_type = size_t;

	result_type operator()(const string& a) const
		{ return a.size(); }

	result_type operator()(const int& a) const
		{ return a; }
};

struct recursive_size_visitor {
	using result_type = size_t;

	result_type operator()(const myvariant& a) const
		{ return visit(size_visitor(), a); }

	result_type operator()(const vector<myvariant>& a) const
		{
		size_t rval = 0;
		for ( const auto& v : a )
			rval += visit(size_visitor{}, v);
		return rval;
		}
};

struct stateful {
	using result_type = void;

	template <typename T>
	result_type operator()(T&)
		{ ++state; }

	int state = 0;
};

struct doppler {
	using result_type = void;

	template <typename T>
	result_type operator()(T& x) const
		{ x += x; }
};

struct binary {
	using result_type = bool;

	template <typename T>
	result_type operator()(const T&, const T&) const
		{ return true; }

	template <typename T, typename U>
	result_type operator()(const T&, const U&) const
		{ return false; }
};

struct ternary {
	using result_type = double;

	template <typename T, typename U>
	result_type operator()(bool c, T const& t, U const& f) const
		{ return c ? t : f; }

	template <typename T, typename U, typename V>
	result_type operator()(T const&, U const&, V const&) const
		{ return 42; }
};

struct reference_returner {
	using result_type = const double&;

	template <typename T>
	result_type operator()(T const&) const
		{
		static constexpr double nada = 0.0;
		return nada;
		}

	result_type operator()(double const& d) const
		{ return d; }
};

int main()
	{
	// Factory construction.
	using pair = default_variant<double, int>;
	BROKER_TEST(get<double>(pair::make(0)));
	BROKER_TEST(get<int>(pair::make(1)));

	// Relational operators.
	pair p0{42};
	pair p1{42.0};
	pair p2{1337};
	pair p3{4.2};

	BROKER_TEST(p0 != p1);
	BROKER_TEST(p0 != p2);
	BROKER_TEST(p0 != p3);
	BROKER_TEST(p1 != p3);

	p1 = 4.2;
	BROKER_TEST(p1 == p3);

	BROKER_TEST(! (p1 < p3 || p1 > p3));
	BROKER_TEST(p1 < p2);
	BROKER_TEST(p2 > p1);
	BROKER_TEST(p0 < p2);

	// double types are less than int types within the variant ordering.
	BROKER_TEST(p1 < p0);
	BROKER_TEST(p1 < p2);
	BROKER_TEST(p3 < p2);

	// Discriminator introspection
	using triple = default_variant<int, double, std::string>;
	triple t0{42};
	triple t1{4.2};
	triple t2{"42"};
	BROKER_TEST(t0.which() == 0);
	BROKER_TEST(t1.which() == 1);
	BROKER_TEST(t2.which() == 2);

	// Type-based access
	BROKER_TEST(is<int>(t0));
	BROKER_TEST(*get<int>(t0) == 42);
	BROKER_TEST(is<double>(t1));
	BROKER_TEST(*get<double>(t1) == 4.2);
	BROKER_TEST(is<string>(t2));
	BROKER_TEST(*get<string>(t2) == "42");

	// Assignment
	*get<int>(t0) = 1337;
	*get<double>(t1) = 1.337;
	std::string leet{"1337"};
	*get<std::string>(t2) = std::move(leet);
	BROKER_TEST(*get<int>(t0) == 1337);
	BROKER_TEST(*get<double>(t1) == 1.337);
	BROKER_TEST(*get<std::string>(t2) == "1337");

	// Unary visitation.
	stateful v;
	visit(v, t0);           // lvalue
	visit(stateful{}, t0);  // rvalue
	visit(doppler{}, t0);
	BROKER_TEST(*get<int>(t0) == 1337 * 2);

	// Binary visitation
	BROKER_TEST(! visit(binary{}, t0, t1));
	BROKER_TEST(! visit(binary{}, t1, t0));
	BROKER_TEST(! visit(binary{}, t0, t2));
	BROKER_TEST(visit(binary{}, t0, triple{84}));

	// Ternary visitation
	using trio = default_variant<bool, double, int>;
	BROKER_TEST(visit(ternary{}, trio{true}, trio{4.2}, trio{42}) == 4.2);
	BROKER_TEST(visit(ternary{}, trio{false}, trio{4.2}, trio{1337}) == 1337.0);

	// Delayed visitation
	std::vector<default_variant<double, int>> doubles;
	stateful s;
	doubles.emplace_back(1337);
	doubles.emplace_back(4.2);
	doubles.emplace_back(42);
	std::for_each(doubles.begin(), doubles.end(), apply_visitor(std::ref(s)));
	BROKER_TEST(s.state == 3);
	std::for_each(doubles.begin(), doubles.end(), apply_visitor(doppler{}));
	BROKER_TEST(*get<int>(doubles[2]) == 84);

	// Custom tag.
	variant<tt, int, string, double> cv{"yo"};
	BROKER_TEST(cv.which() == tt::one);

	// Recursive.
	recursive r{"yo"};
	BROKER_TEST(visit(recursive_size_visitor(), r) == 2);
	r = vector<myvariant>{101, "let's", 32, "go"};
	BROKER_TEST(visit(recursive_size_visitor(), r) == (101 + 5 + 32 + 2));

	return BROKER_TEST_RESULT();
	}
