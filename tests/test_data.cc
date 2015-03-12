#include "broker/data.hh"
#include "testsuite.h"

using namespace std;
using namespace broker;

int main()
	{
	BROKER_TEST(data(true) != data(false));
	BROKER_TEST(data(1) != data(true));
	BROKER_TEST(data(-1) != data(1));
	BROKER_TEST(data(1) != data(1u)); // type matters
	BROKER_TEST(data(1.111) != data(1.11));
	BROKER_TEST(data(1.111) == data(1.111));

	broker::vector myvec{1u, 2u, 3u};

	for ( size_t i = 0; i < myvec.size(); ++i )
		BROKER_TEST(data(i + 1) == myvec[i]);

	broker::set myset{7, 13, 37, 42, 1156};
	BROKER_TEST(myset.find(101) == myset.end());
	BROKER_TEST(myset.find(42) != myset.end());

	broker::table mytable{
		make_pair(2, "shoot at it "),
		make_pair(3, "until it dies"),
		make_pair(1, "to defeat the cyberdemon "),
	};

	string protip;
	for ( const auto& i : mytable )
		protip += *get<string>(i.second);

	BROKER_TEST(protip == "to defeat the cyberdemon shoot at it until it dies");

	record::field f0(1.1);
	record::field f1;
	record::field f2(3);
	record::field f1_dup = f1;
	record::field f1_diff("hi");
	record::field f2_dup = f2;
	record::field f2_diff(false);

	BROKER_TEST(f0 != f1);
	BROKER_TEST(f0 != f2);
	BROKER_TEST(f1 == f1_dup);
	BROKER_TEST(f1 != f1_diff);
	BROKER_TEST(f2 == f2_dup);
	BROKER_TEST(f2 != f2_diff);
	BROKER_TEST(! f1);

	broker::record r1({record::field(data("ddd")),
	                   record::field(),
	                   record::field(data(333))});
	BROKER_TEST(r1.size() == 3);
	broker::record r2 = r1;
	BROKER_TEST(r1.size() == r2.size() && r1.size() == 3);
	BROKER_TEST(r1 == r2);
	r2 = broker::record({f0, f1, f2});
	BROKER_TEST(r2.size() == 3);
	BROKER_TEST(r1 != r2);
	broker::record r3({f0, f1, f2_dup});
	BROKER_TEST(r2 == r3);
	BROKER_TEST(! r3.get(1));
	BROKER_TEST(r3.get(2) && *r3.get(2) == 3);

	return BROKER_TEST_RESULT();
	}
