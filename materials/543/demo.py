import json
import tempfile
from pathlib import Path
from locks import LockManager
from store import Store


def run(root):
    locks = LockManager()
    store = Store(Path(root) / "demo.sqlite", locks)
    store.seed({"a": 10, "b": 20})
    store.begin("T1", 1)
    store.begin("T2", 2)
    locks.acquire("T1", "a", "X")
    locks.acquire("T2", "b", "X")
    store.write("T1", "a", 11)
    store.write("T2", "b", 21)
    store.write("T2", "b", 22)
    locks.acquire("T1", "b", "X")
    locks.acquire("T2", "a", "X")
    victim = locks.resolve_one(store)
    result = {"victim": victim, "status_at_release": store.status(victim),
              "waiter_can_read_before_undo": store.read("T1", "b")}
    while store.status(victim)["state"] != "ROLLED_BACK":
        store.rollback_step(victim)
    result["value_after_undo"] = store.read("T1", "b")
    result["expected_restored_value"] = 20
    store.close()
    return result


if __name__ == "__main__":
    with tempfile.TemporaryDirectory() as root:
        print(json.dumps(run(root), sort_keys=True))
