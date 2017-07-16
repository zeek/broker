
import unittest
import sys
import time

import broker

class TestStore(unittest.TestCase):
    def test_basic(self):
        ep1 = broker.Endpoint()
        st1 = ep1.attach("test", broker.Backend.Memory)
        st1.put("key", "value")
        x = st1.get("key")
        self.assertEqual(x, "value")

        # TODO: This is so that the process shuts down.
        st1 = None
        ep1 = None

if __name__ == '__main__':
    #TestStore().test_basic()
    unittest.main(verbosity=3)
