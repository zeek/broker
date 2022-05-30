# @TEST-GROUP: web-socket
#
# @TEST-PORT: BROKER_WEB_SOCKET_PORT
#
# @TEST-EXEC: btest-bg-run node "broker-node --config-file=../node.cfg"
# @TEST-EXEC: btest-bg-run send "python3 ../send.py >send.out"
#
# @TEST-EXEC: $SCRIPTS/wait-for-file send/done 30 || (btest-bg-wait -k 1 && false)
# @TEST-EXEC: btest-diff send/send.out
#
# @TEST-EXEC: btest-bg-wait -k 1

@TEST-START-FILE node.cfg

broker {
  disable-ssl = true
}
topics = ["/test"]
verbose = true

@TEST-END-FILE

@TEST-START-FILE send.py

import asyncio, websockets, os, json, sys, traceback

ws_port = os.environ['BROKER_WEB_SOCKET_PORT'].split('/')[0]

ws_url = f'ws://localhost:{ws_port}/v1/messages/json'

msg = {
    'topic': '/test',
    '@data-type': 'phone',
    "data": '555-0123'
}

async def do_run():
    # Try up to 30 times.
    connected  = False
    for i in range(30):
        try:
            ws = await websockets.connect(ws_url)
            connected  = True
            await ws.send('["/test"]')
            await ws.recv() # wait for ACK
            await ws.send(json.dumps(msg)) # send malformed input
            # wait for error message and drop details from the error string that
            # are likely to change in the future
            err_json = await ws.recv()
            err = json.loads(err_json)
            want = 'input #1 contained invalid data'
            got = err['context']
            if got.startswith(want):
                err['context'] = want
                print(json.dumps(err, sort_keys=True, indent=2))
            else:
                print(f'*** expected description "{want}", got: {got}')
            await ws.close()
            # tell btest we're done
            with open('done', 'w') as f:
                f.write('done')
            sys.exit()
        except:
            if not connected:
                print(f'failed to connect to {ws_url}, try again', file=sys.stderr)
                time.sleep(1)
            else:
                traceback.print_exc()
                sys.exit()

loop = asyncio.get_event_loop()
loop.run_until_complete(do_run())

@TEST-END-FILE
