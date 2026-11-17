# Lock service and undo store

Python 3.11, standard library only. Run `python3 demo.py` from this directory.
The demo uses a temporary real SQLite database and prints the current incident.
`locks.py` also serves clients which only use locks on arbitrary resource names;
`store.py` serves logical multistep row transactions using those locks.
The SQLite transactions protect each physical operation; logical user transactions
span calls and have their own undo records. These are distinct transaction scopes.

The starting point is deliberately small. It is not a production database and
does not promise multi-process coordination or crash recovery for lock ownership.
Follow the task prompt for required behavior and supported interleavings.
