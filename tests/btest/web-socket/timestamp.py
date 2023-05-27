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

import asyncio, websockets, os, time, json, sys, re


from datetime import datetime

ws_port = os.environ['BROKER_WEB_SOCKET_PORT'].split('/')[0]

ws_url = f'ws://localhost:{ws_port}/v1/messages/json'

# Expected timestamps in the message.
timestamps = [
    # using UTC (note: 'Z' is not supported on older Python versions)
    '2020-02-02T02:02:02+00:00',
    '2020-02-02T02:02:02.1+00:00',
    '2020-02-02T02:02:02.12+00:00',
    '2020-02-02T02:02:02.123+00:00',
    '2020-02-02T02:02:02.1234+00:00',
    '2020-02-02T02:02:02.12345+00:00',
    '2020-02-02T02:02:02.123456+00:00',
    # using positive offset from UTC
    '2020-02-02T02:02:02+02:00',
    '2020-02-02T02:02:02.1+02:00',
    '2020-02-02T02:02:02.12+02:00',
    '2020-02-02T02:02:02.123+02:00',
    '2020-02-02T02:02:02.1234+02:00',
    '2020-02-02T02:02:02.12345+02:00',
    '2020-02-02T02:02:02.123456+02:00',
    # using negative offset from UTC
    '2020-02-02T02:02:02-02:00',
    '2020-02-02T02:02:02.1-02:00',
    '2020-02-02T02:02:02.12-02:00',
    '2020-02-02T02:02:02.123-02:00',
    '2020-02-02T02:02:02.1234-02:00',
    '2020-02-02T02:02:02.12345-02:00',
    '2020-02-02T02:02:02.123456-02:00',
]

# tells btest we're done by writing a file
def write_done_file():
    with open('done', 'w') as f:
        f.write('done')

def parse_iso_timestamp(timestamp):
    # replace 'Z' and adjust offset format from '[+-]MM:SS' to '[+-]MMSS'
    # (note: Python versions >= 3.7 don't need this)
    timestamp = timestamp.replace('Z', '+0000')
    timestamp = re.sub(r'([+-]\d{2}):(\d{2})', r'\1\2', timestamp)
    # parse with or without fractional seconds
    formats = ["%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S.%f%z"]
    for format in formats:
        try:
            dt = datetime.strptime(timestamp, format)
            return dt
        except ValueError:
            pass
    raise ValueError(f'failed to parse {timestamp}')

def check_msg(msg):
    try:
        # iterate over the message and compare it to the expected values
        if msg['@data-type'] != 'vector':
            raise RuntimeError(f'unexpected data type for the message: {msg["@data-type"]}')
        items = msg['data']
        num_items = len(items)
        if num_items != len(timestamps):
            raise RuntimeError(f'unexpected number of items: {len(items)} != {len(timestamps)}')
        print(f'got {num_items} items')
        for i in range(num_items):
            item = items[i]
            if item['@data-type'] != 'timestamp':
                raise RuntimeError(f'unexpected data type at index {i}: {item["@data-type"]} != timestamp')
            dt1 = parse_iso_timestamp(item['data'])
            dt2 = parse_iso_timestamp(timestamps[i])
            if dt1 != dt2:
                raise RuntimeError(f'unexpected timestamp at index {i}: {item["data"]} != {timestamps[i]}')
            print(f'OK: {timestamps[i]}')
    except Exception as e:
        print(f'*** {e}')

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
            # get and verify the message
            msg_json = await ws.recv()
            msg = json.loads(msg_json)
            check_msg(msg)
            # tell btest we're done
            write_done_file()
            await ws.close()
            sys.exit()
        except:
            if not connected:
                print(f'failed to connect to {ws_url}, try again', file=sys.stderr)
                time.sleep(1)
            else:
                write_done_file()
                sys.exit()

loop = asyncio.get_event_loop()
loop.run_until_complete(do_run())

@TEST-END-FILE

@TEST-START-FILE send.py

import asyncio, websockets, os, json, sys

ws_port = os.environ['BROKER_WEB_SOCKET_PORT'].split('/')[0]

ws_url = f'ws://localhost:{ws_port}/v1/messages/json'

# Message with timestamps using the local time zone.
msg = {
    'type': 'data-message',
    'topic': '/test',
    '@data-type': "vector",
    'data': [
        # UTC timestamps

        # timestamp without fractional seconds
        {
            '@data-type': "timestamp",
            "data": "2020-02-02T02:02:02Z"
        },
        # timestamp with fractional seconds (1 digit)
        {
            '@data-type': "timestamp",
            "data": "2020-02-02T02:02:02.1Z"
        },
        # timestamp with fractional seconds (2 digit)
        {
            '@data-type': "timestamp",
            "data": "2020-02-02T02:02:02.12Z"
        },
        # timestamp with fractional seconds (3 digit)
        {
            '@data-type': "timestamp",
            "data": "2020-02-02T02:02:02.123Z"
        },
        # timestamp with fractional seconds (4 digit)
        {
            '@data-type': "timestamp",
            "data": "2020-02-02T02:02:02.1234Z"
        },
        # timestamp with fractional seconds (5 digit)
        {
            '@data-type': "timestamp",
            "data": "2020-02-02T02:02:02.12345Z"
        },
        # timestamp with fractional seconds (6 digit)
        {
            '@data-type': "timestamp",
            "data": "2020-02-02T02:02:02.123456Z"
        },

        # timestamps that use a positive offset from UTC.

        # timestamp without fractional seconds
        {
            '@data-type': "timestamp",
            "data": "2020-02-02T02:02:02+02"
        },
        # timestamp with fractional seconds (1 digit)
        {
            '@data-type': "timestamp",
            "data": "2020-02-02T02:02:02.1+0200"
        },
        # timestamp with fractional seconds (2 digit)
        {
            '@data-type': "timestamp",
            "data": "2020-02-02T02:02:02.12+02:00"
        },
        # timestamp with fractional seconds (3 digit)
        {
            '@data-type': "timestamp",
            "data": "2020-02-02T02:02:02.123+02:00"
        },
        # timestamp with fractional seconds (4 digit)
        {
            '@data-type': "timestamp",
            "data": "2020-02-02T02:02:02.1234+02:00"
        },
        # timestamp with fractional seconds (5 digit)
        {
            '@data-type': "timestamp",
            "data": "2020-02-02T02:02:02.12345+02:00"
        },
        # timestamp with fractional seconds (6 digit)
        {
            '@data-type': "timestamp",
            "data": "2020-02-02T02:02:02.123456+02:00"
        },

        # timestamps that use a negative offset from UTC.

        # timestamp without fractional seconds
        {
            '@data-type': "timestamp",
            "data": "2020-02-02T02:02:02-02"
        },
        # timestamp with fractional seconds (1 digit)
        {
            '@data-type': "timestamp",
            "data": "2020-02-02T02:02:02.1-0200"
        },
        # timestamp with fractional seconds (2 digit)
        {
            '@data-type': "timestamp",
            "data": "2020-02-02T02:02:02.12-02:00"
        },
        # timestamp with fractional seconds (3 digit)
        {
            '@data-type': "timestamp",
            "data": "2020-02-02T02:02:02.123-02:00"
        },
        # timestamp with fractional seconds (4 digit)
        {
            '@data-type': "timestamp",
            "data": "2020-02-02T02:02:02.1234-02:00"
        },
        # timestamp with fractional seconds (5 digit)
        {
            '@data-type': "timestamp",
            "data": "2020-02-02T02:02:02.12345-02:00"
        },
        # timestamp with fractional seconds (6 digit)
        {
            '@data-type': "timestamp",
            "data": "2020-02-02T02:02:02.123456-02:00"
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
