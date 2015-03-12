#include "broker/broker.h"
#include "testsuite.h"

int main()
	{
	BROKER_TEST(broker_init(0) == 0);
	// TODO: add more testing
	return BROKER_TEST_RESULT();
	}
