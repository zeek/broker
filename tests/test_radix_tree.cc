#include "testsuite.h"
#include "../src/util/radix_tree.hh"
#include <string>
#include <deque>
#include <set>
#include <sstream>

using namespace std;

using my_radix_tree = broker::util::radix_tree<int>;

static bool check_match(deque<my_radix_tree::iterator> matches,
                        set<pair<string, int>> expected)
	{
	if ( matches.size() != expected.size() )
		return false;

	for ( auto it = expected.begin(); it != expected.end(); ++it )
		{
		auto i = distance(expected.begin(), it);

		if ( matches[i]->first != it->first )
			return false;

		if ( matches[i]->second != it->second )
			return false;
		}

	return true;
	}

static bool find(deque<my_radix_tree::iterator> haystack,
                 pair<string, int> needle)
	{
	for ( const auto& h : haystack )
		if ( h->first == needle.first && h->second == needle.second )
			return true;

	return false;
	}

static void test_insert_very_long()
	{
	broker::util::radix_tree<void*> t;

	unsigned char key1[300] =
	{16,0,0,0,7,10,0,0,0,2,17,10,0,0,0,120,10,0,0,0,120,10,0,
	 0,0,216,10,0,0,0,202,10,0,0,0,194,10,0,0,0,224,10,0,0,0,
	 230,10,0,0,0,210,10,0,0,0,206,10,0,0,0,208,10,0,0,0,232,
	 10,0,0,0,124,10,0,0,0,124,2,16,0,0,0,2,12,185,89,44,213,
	 251,173,202,211,95,185,89,110,118,251,173,202,199,101,0,
	 8,18,182,92,236,147,171,101,150,195,112,185,218,108,246,
	 139,164,234,195,58,177,0,8,16,0,0,0,2,12,185,89,44,213,
	 251,173,202,211,95,185,89,110,118,251,173,202,199,101,0,
	 8,18,180,93,46,151,9,212,190,95,102,178,217,44,178,235,
	 29,190,218,8,16,0,0,0,2,12,185,89,44,213,251,173,202,
	 211,95,185,89,110,118,251,173,202,199,101,0,8,18,180,93,
	 46,151,9,212,190,95,102,183,219,229,214,59,125,182,71,
	 108,180,220,238,150,91,117,150,201,84,183,128,8,16,0,0,
	 0,2,12,185,89,44,213,251,173,202,211,95,185,89,110,118,
	 251,173,202,199,101,0,8,18,180,93,46,151,9,212,190,95,
	 108,176,217,47,50,219,61,134,207,97,151,88,237,246,208,
	 8,18,255,255,255,219,191,198,134,5,223,212,72,44,208,
	 250,180,14,1,0,0,8, '\0'};
	unsigned char key2[303] =
	{16,0,0,0,7,10,0,0,0,2,17,10,0,0,0,120,10,0,0,0,120,10,0,
	 0,0,216,10,0,0,0,202,10,0,0,0,194,10,0,0,0,224,10,0,0,0,
	 230,10,0,0,0,210,10,0,0,0,206,10,0,0,0,208,10,0,0,0,232,
	 10,0,0,0,124,10,0,0,0,124,2,16,0,0,0,2,12,185,89,44,213,
	 251,173,202,211,95,185,89,110,118,251,173,202,199,101,0,
	 8,18,182,92,236,147,171,101,150,195,112,185,218,108,246,
	 139,164,234,195,58,177,0,8,16,0,0,0,2,12,185,89,44,213,
	 251,173,202,211,95,185,89,110,118,251,173,202,199,101,0,
	 8,18,180,93,46,151,9,212,190,95,102,178,217,44,178,235,
	 29,190,218,8,16,0,0,0,2,12,185,89,44,213,251,173,202,
	 211,95,185,89,110,118,251,173,202,199,101,0,8,18,180,93,
	 46,151,9,212,190,95,102,183,219,229,214,59,125,182,71,
	 108,180,220,238,150,91,117,150,201,84,183,128,8,16,0,0,
	 0,3,12,185,89,44,213,251,133,178,195,105,183,87,237,150,
	 155,165,150,229,97,182,0,8,18,161,91,239,50,10,61,150,
	 223,114,179,217,64,8,12,186,219,172,150,91,53,166,221,
	 101,178,0,8,18,255,255,255,219,191,198,134,5,208,212,72,
	 44,208,250,180,14,1,0,0,8, '\0'};

	BROKER_TEST(t.insert(make_pair(string(key1, key1 + 299), key1)).second);
	BROKER_TEST(t.insert(make_pair(string(key2, key2 + 302), key2)).second);
	BROKER_TEST(! t.insert(make_pair(string(key2, key2 + 302), key2)).second);
	BROKER_TEST(t.size() == 2);
	}

static void test_long_prefix()
	{
	my_radix_tree t{
		{"this:key:has:a:long:prefix:3", 3},
		{"this:key:has:a:long:common:prefix:2", 2},
		{"this:key:has:a:long:common:prefix:1", 1},
	};

	const char *s;

	s = "this:key:has:a:long:common:prefix:1";
	BROKER_TEST(t.find(s)->second == 1);

	s = "this:key:has:a:long:common:prefix:2";
	BROKER_TEST(t.find(s)->second == 2);

	s = "this:key:has:a:long:prefix:3";
	BROKER_TEST(t.find(s)->second == 3);

	BROKER_TEST(check_match(t.prefixed_by("this:key:has"), {
	                        make_pair("this:key:has:a:long:common:prefix:1", 1),
	                        make_pair("this:key:has:a:long:common:prefix:2", 2),
	                        make_pair("this:key:has:a:long:prefix:3", 3)}));

	BROKER_TEST(check_match(t.prefix_of("this:key:has:a:long:prefix:321"), {
	                        make_pair("this:key:has:a:long:prefix:3", 3)
	                        }));
	BROKER_TEST(t.prefix_of("this:key:has:a:long:common:prefix:3").empty());
	BROKER_TEST(check_match(t.prefix_of("this:key:has:a:long:common:prefix:1"),
	                        {
	                        make_pair("this:key:has:a:long:common:prefix:1", 1),
	                        }));
	BROKER_TEST(check_match(t.prefix_of("this:key:has:a:long:common:prefix:2"),
	                        {
	                        make_pair("this:key:has:a:long:common:prefix:2", 2),
	                        }));
	}

static void test_prefix_of()
	{
	my_radix_tree t{make_pair("one", 1)};

	BROKER_TEST(t.prefix_of("").empty());
	BROKER_TEST(t.prefix_of("nope").empty());
	BROKER_TEST(t.prefix_of("on").empty());
	BROKER_TEST(check_match(t.prefix_of("one"),
	                        {
	                        make_pair("one", 1)
	                        }));
	BROKER_TEST(check_match(t.prefix_of("one-hundred"),
	                        {
	                        make_pair("one", 1)
	                        }));
	t["one-hundred"] = 100;
	BROKER_TEST(t.prefix_of("").empty());
	BROKER_TEST(t.prefix_of("nope").empty());
	BROKER_TEST(t.prefix_of("on").empty());

	BROKER_TEST(check_match(t.prefix_of("one"),
	                        {
	                        make_pair("one", 1),
	                        }));
	BROKER_TEST(check_match(t.prefix_of("one-hundred"),
	                        {
	                        make_pair("one", 1),
	                        make_pair("one-hundred", 100)
	                        }));
	t["one-hundred-thousand"] = 100000;
	BROKER_TEST(t.prefix_of("").empty());
	BROKER_TEST(t.prefix_of("nope").empty());
	BROKER_TEST(t.prefix_of("on").empty());

	BROKER_TEST(check_match(t.prefix_of("one"),
	                        {
	                        make_pair("one", 1),
	                        }));
	BROKER_TEST(check_match(t.prefix_of("one-hundred"),
	                        {
	                        make_pair("one", 1),
	                        make_pair("one-hundred", 100)
	                        }));
	BROKER_TEST(check_match(t.prefix_of("one-hundred-thousand"),
	                        {
	                        make_pair("one", 1),
	                        make_pair("one-hundred", 100),
	                        make_pair("one-hundred-thousand", 100000)
	                        }));
	BROKER_TEST(check_match(t.prefix_of("one-hundred-two"),
	                        {
	                        make_pair("one", 1),
	                        make_pair("one-hundred", 100),
	                        }));
	t["two"] = 2;
	BROKER_TEST(t.prefix_of("").empty());
	BROKER_TEST(t.prefix_of("nope").empty());
	BROKER_TEST(t.prefix_of("on").empty());

	BROKER_TEST(check_match(t.prefix_of("one"),
	                        {
	                        make_pair("one", 1),
	                        }));
	BROKER_TEST(check_match(t.prefix_of("one-hundred"),
	                        {
	                        make_pair("one", 1),
	                        make_pair("one-hundred", 100)
	                        }));
	BROKER_TEST(check_match(t.prefix_of("one-hundred-thousand"),
	                        {
	                        make_pair("one", 1),
	                        make_pair("one-hundred", 100),
	                        make_pair("one-hundred-thousand", 100000)
	                        }));
	BROKER_TEST(check_match(t.prefix_of("one-hundred-two"),
	                        {
	                        make_pair("one", 1),
	                        make_pair("one-hundred", 100),
	                        }));
	t["two-fifty"] = 250;
	BROKER_TEST(t.prefix_of("").empty());
	BROKER_TEST(t.prefix_of("nope").empty());
	BROKER_TEST(t.prefix_of("on").empty());

	BROKER_TEST(check_match(t.prefix_of("one"),
	                        {
	                        make_pair("one", 1),
	                        }));
	BROKER_TEST(check_match(t.prefix_of("one-hundred"),
	                        {
	                        make_pair("one", 1),
	                        make_pair("one-hundred", 100)
	                        }));
	BROKER_TEST(check_match(t.prefix_of("one-hundred-thousand"),
	                        {
	                        make_pair("one", 1),
	                        make_pair("one-hundred", 100),
	                        make_pair("one-hundred-thousand", 100000)
	                        }));
	BROKER_TEST(check_match(t.prefix_of("one-hundred-two"),
	                        {
	                        make_pair("one", 1),
	                        make_pair("one-hundred", 100),
	                        }));
	BROKER_TEST(check_match(t.prefix_of("two-fifty-five"),
	                        {
	                        make_pair("two", 2),
	                        make_pair("two-fifty", 250),
	                        }));
	t["zero"] = 0;
	BROKER_TEST(t.prefix_of("").empty());
	BROKER_TEST(t.prefix_of("nope").empty());
	BROKER_TEST(t.prefix_of("on").empty());

	BROKER_TEST(check_match(t.prefix_of("one"),
	                        {
	                        make_pair("one", 1),
	                        }));
	BROKER_TEST(check_match(t.prefix_of("one-hundred"),
	                        {
	                        make_pair("one", 1),
	                        make_pair("one-hundred", 100)
	                        }));
	BROKER_TEST(check_match(t.prefix_of("one-hundred-thousand"),
	                        {
	                        make_pair("one", 1),
	                        make_pair("one-hundred", 100),
	                        make_pair("one-hundred-thousand", 100000)
	                        }));
	BROKER_TEST(check_match(t.prefix_of("one-hundred-two"),
	                        {
	                        make_pair("one", 1),
	                        make_pair("one-hundred", 100),
	                        }));
	BROKER_TEST(check_match(t.prefix_of("two-fifty-five"),
	                        {
	                        make_pair("two", 2),
	                        make_pair("two-fifty", 250),
	                        }));
	t[""] = -1;
	BROKER_TEST(check_match(t.prefix_of(""),
	                        {
	                        make_pair("", -1),
	                        }));
	BROKER_TEST(check_match(t.prefix_of("nope"),
	                        {
	                        make_pair("", -1),
	                        }));
	BROKER_TEST(check_match(t.prefix_of("on"),
	                        {
	                        make_pair("", -1),
	                        }));

	BROKER_TEST(check_match(t.prefix_of("one"),
	                        {
	                        make_pair("", -1),
	                        make_pair("one", 1),
	                        }));
	BROKER_TEST(check_match(t.prefix_of("one-hundred"),
	                        {
	                        make_pair("", -1),
	                        make_pair("one", 1),
	                        make_pair("one-hundred", 100)
	                        }));
	BROKER_TEST(check_match(t.prefix_of("one-hundred-thousand"),
	                        {
	                        make_pair("", -1),
	                        make_pair("one", 1),
	                        make_pair("one-hundred", 100),
	                        make_pair("one-hundred-thousand", 100000)
	                        }));
	BROKER_TEST(check_match(t.prefix_of("one-hundred-two"),
	                        {
	                        make_pair("", -1),
	                        make_pair("one", 1),
	                        make_pair("one-hundred", 100),
	                        }));
	BROKER_TEST(check_match(t.prefix_of("two-fifty-five"),
	                        {
	                        make_pair("", -1),
	                        make_pair("two", 2),
	                        make_pair("two-fifty", 250),
	                        }));
	}

static void test_prefix_match()
	{
	my_radix_tree t{
		{"api.foo.bar", 1},
		{"api.foo.baz", 2},
		{"api.foe.fum", 3},
		{"abc.123.456", 4},
		{"api.foo", 5},
		{"api", 6},
	};

	BROKER_TEST(check_match(t.prefixed_by("api"), {
	                        make_pair("api", 6),
	                        make_pair("api.foe.fum", 3),
	                        make_pair("api.foo", 5),
	                        make_pair("api.foo.bar", 1),
	                        make_pair("api.foo.baz", 2),
	                        }));

	BROKER_TEST(check_match(t.prefixed_by("a"), {
	                        make_pair("abc.123.456", 4),
	                        make_pair("api", 6),
	                        make_pair("api.foe.fum", 3),
	                        make_pair("api.foo", 5),
	                        make_pair("api.foo.bar", 1),
	                        make_pair("api.foo.baz", 2),
	                        }));

	BROKER_TEST(t.prefixed_by("b").empty());

	BROKER_TEST(check_match(t.prefixed_by("api."), {
	                        make_pair("api.foe.fum", 3),
	                        make_pair("api.foo", 5),
	                        make_pair("api.foo.bar", 1),
	                        make_pair("api.foo.baz", 2),
	                        }));

	BROKER_TEST(check_match(t.prefixed_by("api.foo.bar"), {
	                        make_pair("api.foo.bar", 1),
	                        }));

	BROKER_TEST(t.prefixed_by("api.end").empty());

	BROKER_TEST(check_match(t.prefixed_by(""), {
	                        make_pair("abc.123.456", 4),
	                        make_pair("api", 6),
	                        make_pair("api.foe.fum", 3),
	                        make_pair("api.foo", 5),
	                        make_pair("api.foo.bar", 1),
	                        make_pair("api.foo.baz", 2),
	                        }));

	BROKER_TEST(check_match(t.prefix_of("api.foo.bar.baz"), {
	                        make_pair("api", 6),
	                        make_pair("api.foo", 5),
	                        make_pair("api.foo.bar", 1),
	                        }));
	BROKER_TEST(check_match(t.prefix_of("api.foo.fum"), {
	                        make_pair("api", 6),
	                        make_pair("api.foo", 5),
	                        }));
	BROKER_TEST(t.prefix_of("").empty());
	}

static void test_many_keys()
	{
	my_radix_tree t;
	deque<string> keys;
	deque<int> values;

	for ( int i = 0; i < 1000; ++i )
		{
		stringstream ss;
		ss << i;
		keys.push_back(ss.str());
		values.push_back(i);
		BROKER_TEST(t.insert(make_pair(keys[i], values[i])).second);
		}

	BROKER_TEST(t.size() == 1000);

	for ( const auto& i : values )
		BROKER_TEST(t.find(keys[i])->second == i);

	auto matches = t.prefixed_by("1");
	BROKER_TEST(matches.size() == 1 + 10 + 100);
	for ( const auto& p : matches )
		BROKER_TEST(p->first[0] == '1');

	matches = t.prefix_of("109876");
	BROKER_TEST(check_match(matches, {
	                        make_pair("109", 109),
	                        make_pair("10", 10),
	                        make_pair("1", 1),
	                        }));

	matches = t.prefix_of("54321");
	BROKER_TEST(check_match(matches, {
	                        make_pair("543", 543),
	                        make_pair("54", 54),
	                        make_pair("5", 5),
	                        }));

	for ( int i = 0; i < 500; ++i )
		{
		BROKER_TEST(t.erase(keys[i]) == 1);
		BROKER_TEST(t.erase(keys[i]) == 0);
		}

	BROKER_TEST(t.size() == 500);

	for ( const auto& i : values )
		{
		if ( i < 500 )
			BROKER_TEST(t.find(keys[i]) == t.end());
		else
			BROKER_TEST(t.find(keys[i])->second == i);
		}

	for ( int i = 500; i < 995; ++i )
		{
		BROKER_TEST(t.erase(keys[i]) == 1);
		BROKER_TEST(t.erase(keys[i]) == 0);
		}

	for ( const auto& i : values )
		{
		if ( i < 995 )
			BROKER_TEST(t.find(keys[i]) == t.end());
		else
			BROKER_TEST(t.find(keys[i])->second == i);
		}

	for ( int i = 0; i < 1000; ++i )
		{
		if ( i < 995 )
			BROKER_TEST(t.insert(make_pair(keys[i], values[i])).second);
		else
			BROKER_TEST(! t.insert(make_pair(keys[i], values[i])).second);
		}

	BROKER_TEST(t.size() == 1000);

	matches = t.prefixed_by("9");
	BROKER_TEST(matches.size() == 1 + 10 + 100);
	for ( const auto& p : matches )
		BROKER_TEST(p->first[0] == '9');

	matches = t.prefix_of("54321");
	BROKER_TEST(check_match(matches, {
	                        make_pair("543", 543),
	                        make_pair("54", 54),
	                        make_pair("5", 5),
	                        }));

	t.clear();
	BROKER_TEST(t.size() == 0);
	}

static void test_dense_nodes()
	{
	my_radix_tree t;
	auto idx = 0;

	for ( auto i = 0; i < 256; ++i )
		for ( auto j = 0; j < 256; ++j )
			for ( auto k = 0; k < 10; ++k )
				{
				stringstream ss;
				ss.put(i).put(j);
				ss << k;
				BROKER_TEST(t.insert(make_pair(ss.str(), idx)).second);
				++idx;
				}

	BROKER_TEST(t.size() == 256 * 256 * 10);
	BROKER_TEST(t.prefixed_by("a").size() == 256 * 10);
	BROKER_TEST(t.prefix_of("ab0123").size() == 1);
	BROKER_TEST(t.find("az5")->second == 'a' * 256 * 10 + 'z' * 10 + 5);

	for ( auto i = 0; i < 256; ++i )
		for ( auto j = 0; j < 256; ++j )
			for ( auto k = 0; k < 10; ++k )
				{
				stringstream ss;
				ss.put(i).put(j);
				ss << k;
				string s = ss.str();

				BROKER_TEST(t.find(s)->second == i * 256 * 10 + j * 10 + k);
				if ( i == 'b' && j == 'r' ) continue;
				BROKER_TEST(t.erase(s));
				}

	BROKER_TEST(t.size() == 10);

	for ( auto i = 0; i < 10; ++i )
		{
		stringstream ss;
		ss << "br" << i;
		BROKER_TEST(t.find(ss.str())->second == 'b' * 256 * 10 + 'r' * 10 + i);
		}

	BROKER_TEST(t.prefixed_by("b").size() == 10);
	BROKER_TEST(t.prefixed_by("br").size() == 10);
	BROKER_TEST(t.prefixed_by("br0").size() == 1);
	BROKER_TEST(t.prefix_of("br0").size() == 1);
	}

int main()
	{
	my_radix_tree tree;
	BROKER_TEST(tree.empty());
	tree["apache"]    = 0;
	tree["afford"]    = 1;
	tree["available"] = 2;
	tree["affair"]    = 3;
	tree["avenger"]   = 4;
	tree["binary"]    = 5;
	tree["bind"]      = 6;
	tree["brother"]   = 7;
	tree["brace"]     = 8;
	tree["blind"]     = 9;

	/*
              (root)
           /          \
          a             b
         /|\          /   |
       ff | v       in     |\
     / |  |   \    /  \    |  r
ord$ air$ |    \  ary$ d$  |   \
          |    |\         lind$ \
        pache$ | enger$         | \
               ailable$       ace$ other$
	 */

	my_radix_tree copy(tree);
	my_radix_tree other_copy = tree;
	BROKER_TEST(tree.size() == 10);
	BROKER_TEST(copy.size() == 10);
	BROKER_TEST(other_copy.size() == 10);
	BROKER_TEST(tree == copy);
	BROKER_TEST(tree == other_copy);

	for ( const auto& p : tree )
		{
		BROKER_TEST(copy.find(p.first) != tree.end());
		BROKER_TEST(other_copy.find(p.first) != tree.end());
		}

	BROKER_TEST(copy.erase("binary") == 1);
	BROKER_TEST(other_copy.erase("binary") == 1);
	BROKER_TEST(tree != copy);
	BROKER_TEST(tree != other_copy);
	BROKER_TEST(copy == other_copy);

	auto matches = tree.prefixed_by("nothing");
	BROKER_TEST(matches.empty());

	matches = tree.prefixed_by("aff");
	BROKER_TEST(check_match(matches,
	                        {make_pair("affair", 3),
	                         make_pair("afford", 1)}));
	matches = tree.prefixed_by("bi");
	BROKER_TEST(check_match(matches,
	                        {make_pair("binary", 5),
	                         make_pair("bind", 6)}));
	matches = tree.prefixed_by("a");
	BROKER_TEST(check_match(matches,
	                        {make_pair("apache", 0),
	                         make_pair("afford", 1),
	                         make_pair("available", 2),
	                         make_pair("affair", 3),
	                         make_pair("avenger", 4)}));
	matches = tree.prefixed_by("");
	BROKER_TEST(tree.size() == 10);
	BROKER_TEST(matches.size() == tree.size());

	for ( const auto& m : matches )
		{
		auto it = tree.find(m->first);
		BROKER_TEST(it != tree.end());
		BROKER_TEST(it->first == m->first);
		BROKER_TEST(it->second == m->second);
		}

	for ( const auto& p : tree )
		BROKER_TEST(find(matches, p));

	BROKER_TEST(tree.insert(make_pair("apache", -1)).second == false);
	BROKER_TEST(tree.size() == 10);
	BROKER_TEST(tree.find("apache")->second == 0);
	tree["apache"] = -1;
	BROKER_TEST(tree.find("apache")->second == -1);
	BROKER_TEST(tree.size() == 10);

	BROKER_TEST(tree.insert(make_pair("alien", 0)).second == true);
	BROKER_TEST(tree.size() == 11);
	BROKER_TEST(tree.find("alien")->second == 0);

	BROKER_TEST(tree.insert(make_pair("bro", 42)).second == true);
	BROKER_TEST(tree.size() == 12);

	matches = tree.prefixed_by("b");
	BROKER_TEST(check_match(matches,
	                        {make_pair("bind", 6),
	                         make_pair("binary", 5),
	                         make_pair("blind", 9),
	                         make_pair("brace", 8),
	                         make_pair("bro", 42),
	                         make_pair("brother", 7)}));

	BROKER_TEST(tree.erase("nope") == 0);
	BROKER_TEST(tree.erase("a") == 0);
	BROKER_TEST(tree.size() == 12);
	BROKER_TEST(tree.erase("bro") == 1);
	BROKER_TEST(tree.size() == 11);

	matches = tree.prefixed_by("b");
	BROKER_TEST(check_match(matches,
	                        {make_pair("bind", 6),
	                         make_pair("binary", 5),
	                         make_pair("blind", 9),
	                         make_pair("brace", 8),
	                         make_pair("brother", 7)}));

	BROKER_TEST(tree.insert(make_pair("bro", 42)).second == true);
	BROKER_TEST(tree.size() == 12);
	BROKER_TEST(tree.erase("brother") == 1);

	matches = tree.prefixed_by("b");
	BROKER_TEST(check_match(matches,
	                        {make_pair("bind", 6),
	                         make_pair("binary", 5),
	                         make_pair("blind", 9),
	                         make_pair("brace", 8),
	                         make_pair("bro", 42)}));

	BROKER_TEST(tree.erase("brace") == 1);
	BROKER_TEST(tree.erase("bind") == 1);
	BROKER_TEST(tree.erase("blind") == 1);
	BROKER_TEST(tree.erase("binary") == 1);

	tree.clear();
	BROKER_TEST(tree.size() == 0);

	BROKER_TEST(tree.insert(make_pair("bro", 42)).second == true);
	BROKER_TEST(tree.insert(make_pair("bros", 1)).second == true);
	BROKER_TEST(tree.insert(make_pair("brother", 2)).second == true);
	BROKER_TEST(tree.size() == 3);

	matches = tree.prefixed_by("bro");
	BROKER_TEST(check_match(matches,
	                        {make_pair("bro", 42),
	                         make_pair("bros", 1),
	                         make_pair("brother", 2)}));

	matches = tree.prefix_of("bros");
	BROKER_TEST(check_match(matches,
	                        {make_pair("bro", 42),
	                         make_pair("bros", 1),
	                        }));

	matches = tree.prefix_of("brothers");
	BROKER_TEST(check_match(matches,
	                        {make_pair("bro", 42),
	                         make_pair("brother", 2),
	                        }));

	BROKER_TEST(tree.erase("brother") == 1);
	BROKER_TEST(tree.erase("bros") == 1);
	BROKER_TEST(tree.size() == 1);

	BROKER_TEST(tree.find("bro") != tree.end());
	BROKER_TEST(tree.find("bro")->first == "bro");
	BROKER_TEST(tree.find("bro")->second == 42);

	matches = tree.prefixed_by("bro");
	BROKER_TEST(check_match(matches, {make_pair("bro", 42)}));

	BROKER_TEST(tree.erase("bro") == 1);
	matches = tree.prefixed_by("");
	BROKER_TEST(matches.empty());

	test_long_prefix();
	test_insert_very_long();
	test_prefix_match();
	test_prefix_of();
	test_many_keys();
	test_dense_nodes();

	return BROKER_TEST_RESULT();
	}
