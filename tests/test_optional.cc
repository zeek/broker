#include "broker/maybe.hh"

#include "testsuite.h"

using namespace std;
using namespace broker;

int main() {
  maybe<int> i0 = 11;
  maybe<int> i1 = 55;
  maybe<int> i2;
  maybe<int> i3;
  maybe<int> i4 = 11;

  BROKER_TEST(i0.valid());
  BROKER_TEST(!i2);
  BROKER_TEST(i2.empty());

  BROKER_TEST(i0 != i1);
  BROKER_TEST(i1 != i0);
  BROKER_TEST(i0 == i4);
  BROKER_TEST(i4 == i0);

  BROKER_TEST(i0 != i2);
  BROKER_TEST(i2 != i0);

  BROKER_TEST(i2 == i3);
  BROKER_TEST(i3 == i2);

  BROKER_TEST(i0 < i1);
  BROKER_TEST(i1 > i0);

  return BROKER_TEST_RESULT();
}
