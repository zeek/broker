*** Settings ***
Library             OperatingSystem
Library             String
Library             Process
Library             Collections
Library             WebSocketClient
Variables           WebSocketClientsVars.py

Test Setup          Start Node
Test Teardown       Stop Node


*** Variables ***
${brokerNodeBin}    broker-node
${webSocketPort}    2244
${webSocketUrl}     ws://localhost:${webSocketPort}/v1/events/json


*** Test Cases ***
Clients Receive Events From Other Clients
    Log    Connect producer and consumer to the WebSocket server.
    ${producerSock}=    Connect To Node
    ${consumerSock}=    Connect To Node
    Log    Send the filters to the server.
    ${producerFilter}=    Get File    ${CURDIR}/producer-1/filter.json
    ${consumerFilter}=    Get File    ${CURDIR}/consumer-1/filter.json
    WebSocketClient.Send    ${producerSock}    ${producerFilter}
    WebSocketClient.Send    ${consumerSock}    ${consumerFilter}
    Log    Receive the ACK on both WebSocket connections.
    ${producerAck}=    WebSocketClient.Recv    ${producerSock}
    ${consumerAck}=    WebSocketClient.Recv    ${consumerSock}
    Should Be An Ack Message    ${producerAck}
    Should Be An Ack Message    ${consumerAck}
    Log    Send all messages for the publisher.
    ${messagesStr}=    Get File    ${CURDIR}/producer-1/send.json
    ${messages}=    Split String    ${messagesStr}    NEXT_MESSAGE
    FOR    ${message}    IN    @{messages}
        WebSocketClient.Send    ${producerSock}    ${message}
    END
    Log    Receive messages at the consumer.
    FOR    ${message}    IN    @{messages}
        ${found}=    WebSocketClient.Recv    ${consumerSock}
        ${foundDict}=    Parse Json Input    ${found}
        ${messageDict}=    Parse Json Input    ${message}
        Set To Dictionary    ${messageDict}    type=event
        Dictionaries Should Be Equal    ${foundDict}    ${messageDict}
    END
    Log    Disconnect sockets.
    WebSocketClient.Close    ${producerSock}
    WebSocketClient.Close    ${consumerSock}


*** Keywords ***
Parse Json Input
    [Documentation]    Converts JSON input to a dictionary.
    [Arguments]    ${inputStr}
    ${res}=    evaluate    json.loads('''${inputStr}''')
    [Return]    ${res}

Start Node
    [Documentation]    Starts the Broker Node.
    ${coutFile}=    Set Variable    stdout.broker-node.txt
    ${cerrFile}=    Set Variable    stderr.broker-node.txt
    ${hdl}=    Start Process
    ...    ${brokerNodeBin}
    ...    --verbose    --broker.web-socket.port\=${webSocketPort}
    ...    --topics\=["web-socket-test"]
    ...    stdout=${coutFile}
    ...    stderr=${cerrFile}
    Set Global Variable    ${brokerNodeProc}    ${hdl}

Stop Node
    [Documentation]    Stops the Broker Node.
    Terminate Process    ${brokerNodeProc}

Do WebSocket Connect
    [Documentation]    Connects to the WebSocket URL and returns the connection.
    Log    Try to connect to ${webSocketUrl}
    ${fd}=    WebSocketClient.connect    ${webSocketUrl}
    [Return]    ${fd}

Connect To Node
    [Documentation]    Connects to broker-node on the configured URL.
    ${conn}=    Wait Until Keyword Succeeds    10 sec    250 msec    Do WebSocket Connect
    [Return]    ${conn}

Get Message Type
    [Documentation]    Parses a node response and extracts only the type.
    [Arguments]    ${inputStr}
    ${input}=    evaluate    json.loads('''${inputStr}''')
    ${res}=    Get From Dictionary    ${input}    type
    [Return]    ${res}

Should Be An Ack Message
    [Documentation]    Checks whether the argument is an ACK message.
    [Arguments]    ${inputStr}
    ${msgType}=    Get Message Type    ${inputStr}
    Should Be Equal    ${msgType}    ack
