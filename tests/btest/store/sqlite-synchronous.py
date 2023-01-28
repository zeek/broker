# This test checks the synchronous modes.
#
# @TEST-GROUP: store
#
# @TEST-EXEC: btest-bg-run prog1 "btest-sqlite-driver --program=../prog1.json > out.txt"
# @TEST-EXEC: btest-bg-run prog2 "btest-sqlite-driver --program=../prog2.json > out.txt"
# @TEST-EXEC: btest-bg-run prog3 "btest-sqlite-driver --program=../prog3.json > out.txt"
# @TEST-EXEC: btest-bg-run prog4 "btest-sqlite-driver --program=../prog4.json > out.txt"
# @TEST-EXEC: btest-bg-wait 30
# @TEST-EXEC: awk 'FNR==1 {print "==> ", FILENAME}{print}' prog*/out.txt > all.txt
# @TEST-EXEC: btest-diff all.txt

# Should report 0 when running the pragma.
@TEST-START-FILE prog1.json
{
    "config": {
        "file-path": "test.db",
        "options": {
            "synchronous": "Broker::SQLITE_SYNCHRONOUS_OFF"
        }
    },
    "commands": [
        ["exec-pragma", ["synchronous"]],
        ["put", ["key1", "value1"]],
        ["get", ["key1"]]
    ]
}
@TEST-END-FILE

# Should report 1 when running the pragma.
@TEST-START-FILE prog2.json
{
    "config": {
        "file-path": "test.db",
        "options": {
            "synchronous": "Broker::SQLITE_SYNCHRONOUS_NORMAL"
        }
    },
    "commands": [
        ["exec-pragma", ["synchronous"]],
        ["put", ["key1", "value1"]],
        ["get", ["key1"]]
    ]
}
@TEST-END-FILE

# Should report 2 when running the pragma.
@TEST-START-FILE prog3.json
{
    "config": {
        "file-path": "test.db",
        "options": {
            "synchronous": "Broker::SQLITE_SYNCHRONOUS_FULL"
        }
    },
    "commands": [
        ["exec-pragma", ["synchronous"]],
        ["put", ["key1", "value1"]],
        ["get", ["key1"]]
    ]
}
@TEST-END-FILE

# Should report 3 when running the pragma.
@TEST-START-FILE prog4.json
{
    "config": {
        "file-path": "test.db",
        "options": {
            "synchronous": "Broker::SQLITE_SYNCHRONOUS_EXTRA"
        }
    },
    "commands": [
        ["exec-pragma", ["synchronous"]],
        ["put", ["key1", "value1"]],
        ["get", ["key1"]]
    ]
}
@TEST-END-FILE
