
import unittest
import multiprocessing
import sys
import time

import broker

def wait_for_connect(es):
    while not es.available():
        time.sleep(1)

    print(repr(es.get()))

class TestCommunication(unittest.TestCase):
    def test_ping(self):
        ep1 = broker.Endpoint()
        es1 = ep1.make_event_subscriber(True)
        port = ep1.listen("127.0.0.1", 0)

        ep2 = broker.Endpoint()
        es2 = ep2.make_event_subscriber(True)
        ep2.peer("127.0.0.1", port, 1.0)

        print(".")
        wait_for_connect(es2)
        print("o")

        ep1 = None
        ep2 = None

if __name__ == '__main__':
  unittest.main(verbosity=3)
