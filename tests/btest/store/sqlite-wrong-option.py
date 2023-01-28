# This test checks that Broker refuses options with a wrong prefix, wrong value
# or wrong type. The commands should never run.
#
# @TEST-GROUP: store
#
# @TEST-EXEC: btest-bg-run prog1 "btest-sqlite-driver --program=../prog1.json > out.txt"
# @TEST-EXEC: btest-bg-run prog2 "btest-sqlite-driver --program=../prog2.json > out.txt"
# @TEST-EXEC: btest-bg-run prog3 "btest-sqlite-driver --program=../prog3.json > out.txt"
# @TEST-EXEC: btest-bg-run prog4 "btest-sqlite-driver --program=../prog4.json > out.txt"
# @TEST-EXEC: btest-bg-run prog5 "btest-sqlite-driver --program=../prog5.json > out.txt"
# @TEST-EXEC: btest-bg-run prog6 "btest-sqlite-driver --program=../prog6.json > out.txt"
# @TEST-EXEC: btest-bg-run prog7 "btest-sqlite-driver --program=../prog6.json > out.txt"
# @TEST-EXEC: btest-bg-wait 30
# @TEST-EXEC: awk 'FNR==1 {print "==> ", FILENAME}{print}' prog*/out.txt > all.txt
# @TEST-EXEC: btest-diff all.txt

# Should fail because prefix is "Foo::" instead of "Broker::".
@TEST-START-FILE prog1.json
{
    "config": {
        "file-path": "test.db",
        "options": {
            "journal_mode": "Foo::SQLITE_JOURNAL_MODE_WAL"
        }
    },
    "commands": [
        ["put", ["key1", "value1"]],
        ["get", ["key1"]]
    ]
}
@TEST-END-FILE

# Should fail because "SQLITE_MODE_SUPER_AWESOME" is not a valid value.
@TEST-START-FILE prog2.json
{
    "config": {
        "file-path": "test.db",
        "options": {
            "journal_mode": "Broker::SQLITE_MODE_SUPER_AWESOME"
        }
    },
    "commands": [
        ["put", ["key1", "value1"]],
        ["get", ["key1"]]
    ]
}
@TEST-END-FILE

# Should fail because prefix is "Foo::" instead of "Broker::".
@TEST-START-FILE prog3.json
{
    "config": {
        "file-path": "test.db",
        "options": {
            "synchronous": "Foo::SQLITE_SYNCHRONOUS_OFF"
        }
    },
    "commands": [
        ["put", ["key1", "value1"]],
        ["get", ["key1"]]
    ]
}
@TEST-END-FILE

# Should fail because "SQLITE_SYNCHRONOUS_CHRONOS" is not a valid value.
@TEST-START-FILE prog4.json
{
    "config": {
        "file-path": "test.db",
        "options": {
            "journal_mode": "Broker::SQLITE_SYNCHRONOUS_CHRONOS"
        }
    },
    "commands": [
        ["put", ["key1", "value1"]],
        ["get", ["key1"]]
    ]
}
@TEST-END-FILE

# Should fail because "journal_mode" does not accept a string (prefixing "STR:"
# will forward the string as-is without converting to an enum value).
@TEST-START-FILE prog5.json
{
    "config": {
        "file-path": "test.db",
        "options": {
            "journal_mode": "STR:foo"
        }
    },
    "commands": [
        ["put", ["key1", "value1"]],
        ["get", ["key1"]]
    ]
}
@TEST-END-FILE

# Should fail because "synchronous" does not accept a string (prefixing "STR:"
# will forward the string as-is without converting to an enum value).
@TEST-START-FILE prog6.json
{
    "config": {
        "file-path": "test.db",
        "options": {
            "synchronous": "STR:foo"
        }
    },
    "commands": [
        ["put", ["key1", "value1"]],
        ["get", ["key1"]]
    ]
}
@TEST-END-FILE

# Should fail because "failure_mode" requires a boolean value.
@TEST-START-FILE prog7.json
{
    "config": {
        "file-path": "test.db",
        "options": {
            "failure_mode": "STR:true"
        }
    },
    "commands": [
        ["put", ["key1", "value1"]],
        ["get", ["key1"]]
    ]
}
@TEST-END-FILE
