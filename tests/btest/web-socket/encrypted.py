# @TEST-GROUP: web-socket
#
# @TEST-PORT: BROKER_WEB_SOCKET_PORT
#
# @TEST-COPY-FILE: ${CERTS_ROOT}/ca.pem
# @TEST-COPY-FILE: ${CERTS_ROOT}/cert.1.pem
# @TEST-COPY-FILE: ${CERTS_ROOT}/key.1.enc.pem
# @TEST-COPY-FILE: ${CERTS_ROOT}/cert.2.pem
# @TEST-COPY-FILE: ${CERTS_ROOT}/key.2.pem
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
  ssl {
    cafile = "../ca.pem"
    certificate = "../cert.1.pem"
    key = "../key.1.enc.pem"
    passphrase = "12345"
  }
}
topics = ["/test"]
verbose = true

@TEST-END-FILE

@TEST-START-FILE recv.py

import asyncio, websockets, os, time, json, sys, ssl

ws_port = os.environ['BROKER_WEB_SOCKET_PORT'].split('/')[0]

ws_url = f'wss://localhost:{ws_port}/v1/messages/json'

ssl_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS)
ssl_ctx.load_verify_locations("../ca.pem")
ssl_ctx.load_cert_chain(certfile="../cert.2.pem", keyfile="../key.2.pem")

async def do_run():
    # Try up to 30 times.
    connected  = False
    for i in range(30):
        try:
            ws = await websockets.connect(ws_url, ssl=ssl_ctx)
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
            # dump messages to stdout (redirected to recv.out)
            for i in range(10):
                msg = await ws.recv()
                print(f'{msg}')
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

import asyncio, websockets, os, json, sys, ssl

ws_port = os.environ['BROKER_WEB_SOCKET_PORT'].split('/')[0]

ws_url = f'wss://localhost:{ws_port}/v1/messages/json'

ssl_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS)
ssl_ctx.load_verify_locations("../ca.pem")
ssl_ctx.load_cert_chain(certfile="../cert.2.pem", keyfile="../key.2.pem")

msg = {
    'type': 'data-message',
    'topic': '/test',
    '@data-type': "count",
    "data": 0
}

async def do_run():
    async with websockets.connect(ws_url, ssl=ssl_ctx) as ws:
      await ws.send('[]')
      await ws.recv() # wait for ACK
      for i in range(10):
          msg['data'] += 1
          await ws.send(json.dumps(msg))
      await ws.close()

loop = asyncio.get_event_loop()
loop.run_until_complete(do_run())

@TEST-END-FILE
