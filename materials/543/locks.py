"""Small single-process lock service; the deadlock-abort path needs repair."""
from collections import defaultdict


class LockManager:
    def __init__(self):
        self.ages = {}
        self.states = {}
        self.holders = defaultdict(dict)
        self.waiters = defaultdict(list)

    def register(self, tx, age):
        if tx in self.states or not isinstance(age, int):
            raise ValueError("duplicate transaction or invalid age")
        self.ages[tx] = age
        self.states[tx] = "ACTIVE"

    @staticmethod
    def _conflict(requested, held):
        return requested == "X" or held == "X"

    def _blockers(self, tx, resource, mode):
        return {other for other, held in self.holders[resource].items()
                if other != tx and self._conflict(mode, held)}

    def acquire(self, tx, resource, mode):
        if self.states.get(tx) != "ACTIVE" or mode not in ("S", "X"):
            raise ValueError("inactive transaction or invalid mode")
        held = self.holders[resource].get(tx)
        if held == "X" or held == mode:
            return True
        request = (tx, mode)
        if request in self.waiters[resource]:
            return False
        if not self.waiters[resource] and not self._blockers(tx, resource, mode):
            self.holders[resource][tx] = mode
            return True
        self.waiters[resource].append(request)
        return False

    def holds(self, tx, resource, mode):
        held = self.holders[resource].get(tx)
        return held == "X" or (mode == "S" and held == "S")

    def _drain(self, resource):
        queue = self.waiters[resource]
        while queue:
            tx, mode = queue[0]
            if self._blockers(tx, resource, mode):
                break
            queue.pop(0)
            self.holders[resource][tx] = mode

    def cancel_pending(self, tx):
        for resource in list(self.waiters):
            self.waiters[resource] = [item for item in self.waiters[resource]
                                      if item[0] != tx]

    def release_all(self, tx):
        self.cancel_pending(tx)
        for resource in set(self.holders) | set(self.waiters):
            self.holders[resource].pop(tx, None)
            self._drain(resource)

    def snapshot(self):
        return {"holders": {r: dict(sorted(v.items())) for r, v in sorted(self.holders.items()) if v},
                "waiters": {r: [list(item) for item in v] for r, v in sorted(self.waiters.items()) if v}}

    def wait_graph(self):
        graph = {tx: set() for tx, state in self.states.items() if state == "ACTIVE"}
        for resource, queue in self.waiters.items():
            for tx, mode in queue:
                # Current implementation sees physical holders, not FIFO predecessors.
                graph[tx].update(self._blockers(tx, resource, mode))
        return {tx: sorted(edges) for tx, edges in sorted(graph.items())}

    def cycle_members(self):
        graph = self.wait_graph()
        cyclic = set()
        for start in graph:
            todo = list(graph[start])
            seen = set()
            while todo:
                current = todo.pop()
                if current == start:
                    cyclic.add(start)
                    break
                if current not in seen:
                    seen.add(current)
                    todo.extend(graph.get(current, []))
        return sorted(cyclic)

    def resolve_one(self, store):
        members = self.cycle_members()
        if not members:
            return None
        victim = max(members, key=lambda tx: (self.ages[tx], tx))
        self.states[victim] = "ABORTING"
        self.cancel_pending(victim)
        store.start_abort(victim)
        # This is the incident path: rollback has only been scheduled here.
        self.release_all(victim)
        return victim

    def finish_abort(self, tx, store):
        status = store.status(tx)
        if status["state"] != "ROLLED_BACK" or status["remaining_undo"]:
            return "WAITING_UNDO"
        self.release_all(tx)
        self.states[tx] = "ENDED"
        return "RELEASED"
