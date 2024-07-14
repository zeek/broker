# @TEST-GROUP: endpoint
#
# @TEST-PORT: BROKER_PORT
#
# Setup 1: establish a peering between two endpoints and then unpeer.
# @TEST-EXEC: btest-bg-run responder "btest-unpeer responder > responder.txt"
# @TEST-EXEC: btest-unpeer originator > originator.txt
# @TEST-EXEC: btest-bg-wait -k 10
# @TEST-EXEC: btest-diff originator.txt
# @TEST-EXEC: btest-diff responder/responder.txt
#
# Setup 2: try to unpeer an endpoint that is not peered.
# @TEST-EXEC: btest-unpeer invalid > invalid.txt
# @TEST-EXEC: btest-diff invalid.txt
