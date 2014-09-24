#include "broker/data.hh"
#include "testsuite.hh"

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

	return BROKER_TEST_RESULT();
	}
