# @TEST-GROUP: web-socket
#
# @TEST-PORT: BROKER_WEB_SOCKET_PORT
#
# @TEST-EXEC: btest-bg-run node "broker-node --config-file=../node.cfg"
# @TEST-EXEC: btest-bg-run recv "python3 ../recv.py >recv.out"
# @TEST-EXEC: $SCRIPTS/wait-for-file recv/ready 15 || (btest-bg-wait -k 1 && false)
#
# @TEST-EXEC: btest-bg-run send "python3 ../send.py"
#
# @TEST-EXEC: $SCRIPTS/wait-for-file recv/done 30 || (btest-bg-wait -k 1 && false)
# @TEST-EXEC: btest-diff recv/recv.out
#
# @TEST-EXEC: btest-bg-wait -k 1

@TEST-START-FILE node.cfg

broker {
  disable-ssl = true
}
topics = ["/test"]
verbose = true

@TEST-END-FILE

@TEST-START-FILE recv.py

import asyncio, websockets, os, time, json, sys

ws_port = os.environ['BROKER_WEB_SOCKET_PORT'].split('/')[0]

ws_url = f'ws://localhost:{ws_port}/v1/messages/json'

async def do_run():
    # Try up to 30 times.
    connected  = False
    for i in range(30):
        try:
            ws = await websockets.connect(ws_url)
            connected  = True
            # send filter and wait for ack
            await ws.send('["/test"]')
            ack_json = await ws.recv()
            ack = json.loads(ack_json)
            if not 'type' in ack or ack['type'] != 'ack':
                print('*** unexpected ACK from server:')
                print(ack_json)
                sys.exit()
            # tell btest to start the sender now
            with open('ready', 'w') as f:
                f.write('ready')
            # the message must be valid JSON
            msg_json = await ws.recv()
            msg = json.loads(msg_json)
            # pretty-print to stdout (redirected to recv.out)
            print(json.dumps(msg, indent=2))
            # tell btest we're done
            with open('done', 'w') as f:
                f.write('done')
            await ws.close()
            sys.exit()
        except:
            if not connected:
                print(f'failed to connect to {ws_url}, try again', file=sys.stderr)
                time.sleep(1)
            else:
                sys.exit()

loop = asyncio.get_event_loop()
loop.run_until_complete(do_run())

@TEST-END-FILE

@TEST-START-FILE send.py

import asyncio, websockets, os, json, sys

ws_port = os.environ['BROKER_WEB_SOCKET_PORT'].split('/')[0]

ws_url = f'ws://localhost:{ws_port}/v1/messages/json'

# Our message with one value of every type.
msg = {
    'type': 'data-message',
    'topic': '/test',
    '@data-type': "vector",
    'data': [
        # min int64
        {
            '@data-type': "integer",
            "data": -9223372036854775808
        },
        # max int64
        {
            '@data-type': "integer",
            "data": 9223372036854775807
        },
        # min uint64
        {
            '@data-type': "count",
            "data": 0
        },
        # max uint64
        {
            '@data-type': "count",
            "data": 18446744073709551615
        },
        # string
        {
            "@data-type": "string",
            "data": "Hello Broker!"
        },
        # none
        {
            "@data-type": "none",
            "data": {}
        },
        # boolean
        {
            "@data-type": "boolean",
            "data": True
        },
        # floating point
        {
            "@data-type": "real",
            "data": -7.5
        },
        # timespan
        {
            "@data-type": "timespan",
            "data": "15ms"
        },
        # enum
        {
            "@data-type": "enum-value",
            "data": "foo"
        },
        # IP address
        {
            "@data-type": "address",
            "data": "2001:db8::"
        },
        # IP subnet
        {
            "@data-type": "subnet",
            "data": "255.255.255.0/24"
        },
        # port
        {
            "@data-type": "port",
            "data": "8080/tcp"
        },
        # set
        {
            "@data-type": "set",
            "data": [
                {
                    "@data-type": "string",
                    "data": "bar"
                },
                {
                    "@data-type": "string",
                    "data": "foo"
                },
            ]
        },
        # table
        {
            "@data-type": "table",
            "data": [
                {
                    "key": {
                        "@data-type": "string",
                        "data": "first-name"
                    },
                    "value": {
                        "@data-type": "string",
                        "data": "John"
                    }
                },
                {
                    "key": {
                        "@data-type": "string",
                        "data": "last-name"
                    },
                    "value": {
                        "@data-type": "string",
                        "data": "Doe"
                    }
                }
            ]
        },
    ],
}

async def do_run():
    async with websockets.connect(ws_url) as ws:
      await ws.send('[]')
      await ws.recv() # wait for ACK
      await ws.send(json.dumps(msg))
      await ws.close()

loop = asyncio.get_event_loop()
loop.run_until_complete(do_run())

@TEST-END-FILE
