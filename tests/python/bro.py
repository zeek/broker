
import unittest
import multiprocessing
import os
import tempfile
import subprocess
import sys
import time

import broker
import broker.bro

BroPing = """
redef Broker::default_connect_retry=1secs;
redef Broker::default_listen_retry=1secs;
redef exit_only_after_terminate = T;

global event_count: int = 0;

global ping: event(s: string, c: int);

event bro_init()
    {
    Broker::subscribe("/test");
    Broker::peer("127.0.0.1", __PORT__/tcp);
    }

function send_event(s: string)
    {
    s += "x";
    local e = Broker::make_event(ping, s, event_count);
    Broker::publish("/test", e);
    ++event_count;
    }

event Broker::peer_added(endpoint: Broker::EndpointInfo, s: string)
    {
    send_event("");
    }

event Broker::peer_lost(endpoint: Broker::EndpointInfo, msg: string)
    {
    terminate();
    }

event pong(s: string, n: int)
    {
    send_event(s);
    }
"""

def RunBro(script, port):
    try:
        tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".bro", delete=False)
        print(script.replace("__PORT__", str(port)), file=tmp)
        tmp.close()
        os.environ["BROPATH"] = "/home/robin/bro/bro-matthias/scripts:/home/robin/bro/bro-matthias/scripts/policy:/home/robin/bro/bro-matthias/scripts/site:/home/robin/bro/bro-matthias/build/scripts"
        subprocess.check_call(["/home/robin/bro/bro-matthias/build/src/bro", "-b", "-B", "broker", tmp.name])
        return True
    except subprocess.CalledProcessError:
        return False
    finally:
        os.unlink(tmp.name)

class TestCommunication(unittest.TestCase):
    def test_ping(self):
        ep = broker.Endpoint()
        sub = ep.make_subscriber("/test")
        port = ep.listen("127.0.0.1", 0)

        p = multiprocessing.Process(target=RunBro, args=(BroPing, port))
        p.daemon=True
        p.start()

        for i in range(0, 6):
            (t, msg) = sub.get()
            ev = broker.bro.Event(msg)
            (s, c) = ev.args

            self.assertEqual(ev.name, "ping")
            self.assertEqual(s, "x" + "Xx" * i)
            self.assertEqual(c, i)

            ev = broker.bro.Event("pong", s + "X", c)
            ep.publish("/test", ev)

        ep.shutdown()

if __name__ == '__main__':
    unittest.main(verbosity=3)
