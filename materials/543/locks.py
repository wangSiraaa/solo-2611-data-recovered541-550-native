"""Single-process lock service with fair FIFO granting and deadlock resolution.

State machine (per transaction, shared verbatim by the SQLite-backed Store):

    ACTIVE --resolve_one--> ABORTING --(all undo applied)--> ENDED
    ACTIVE --commit / explicit release--> ENDED

A victim keeps every *granted* lock for the whole ABORTING phase; only its
not-yet-granted queue requests are stripped immediately.  Locks protecting
rows that are still being undone are therefore never given to a successor
before the rows are restored.  Pure lock-only clients hit the same rules:
their abort has zero business undo steps, so the release happens at once.

Wait edges combine two kinds of blockers:
  * conflicting holders of the resource;
  * the FIFO predecessor(s) that genuinely stop the request advancing under
    fair queueing (an S only conflicts with an X predecessor, an X with every
    predecessor). Compatible S holders that merely share the queue add no edge.
"""
from collections import defaultdict

VALID_MODES = ("S", "X")
# State vocabulary mirrored by the SQLite txns.state column in store.py.
ACTIVE = "ACTIVE"
ABORTING = "ABORTING"          # lock-side state while the Store is ROLLING_BACK
ENDED = "ENDED"


class LockManager:
    def __init__(self):
        self.ages = {}                       # tx -> monotonic age
        self.states = {}                     # tx -> ACTIVE / ABORTING / ENDED
        self.holders = defaultdict(dict)     # resource -> {tx: mode}
        self.waiters = defaultdict(list)     # resource -> [(tx, mode), ...] FIFO
        self._bound = set()                  # tx whose lifecycle is owned by a Store
        self._max_age = None                 # global monotonic-age check

    # ------------------------------------------------------------------ #
    # validation helpers (all reject before any state change)
    # ------------------------------------------------------------------ #
    @staticmethod
    def _tx_name(tx):
        if not isinstance(tx, str) or not tx:
            raise ValueError("transaction identifier must be a non-empty string")

    @staticmethod
    def _resource_name(resource):
        if not isinstance(resource, str) or not resource:
            raise ValueError("resource name must be a non-empty string")

    def register(self, tx, age):
        self._tx_name(tx)
        # bool is an int subclass; reject it explicitly.
        if not isinstance(age, int) or isinstance(age, bool):
            raise ValueError("age must be an integer")
        if tx in self.states:
            raise ValueError(f"transaction {tx!r} already registered")
        if self._max_age is not None and age < self._max_age:
            raise ValueError("age must be monotonically non-decreasing")
        self.ages[tx] = age
        self.states[tx] = ACTIVE
        self._max_age = age

    def bind_store(self, tx):
        """Mark a transaction as a logical Store transaction."""
        self._require_active(tx)
        self._bound.add(tx)

    def _require_active(self, tx):
        self._tx_name(tx)
        state = self.states.get(tx)
        if state is None:
            raise ValueError(f"unknown transaction {tx!r}")
        if state != ACTIVE:
            raise ValueError(f"transaction {tx!r} is {state}, not ACTIVE")

    @staticmethod
    def _conflict(requested, held):
        return requested == "X" or held == "X"

    def _holder_blockers(self, tx, resource, mode):
        return {other for other, held in self.holders[resource].items()
                if other != tx and self._conflict(mode, held)}

    # ------------------------------------------------------------------ #
    # lock requests
    # ------------------------------------------------------------------ #
    def acquire(self, tx, resource, mode):
        """Request mode on resource. True if granted, False if queued/waiting."""
        self._require_active(tx)
        self._resource_name(resource)
        if mode not in VALID_MODES:
            raise ValueError("mode must be 'S' or 'X'")

        held = self.holders[resource].get(tx)
        if held is not None:
            # Idempotent re-request; an X holder asking for S keeps X (no downgrade).
            if held == mode or held == "X":
                return True
        # An upgrade keeps its granted S and queues one upgrade request.
        if any(w == tx and m == mode for w, m in self.waiters[resource]):
            return False

        self.waiters[resource].append((tx, mode))
        self._drain(resource)
        return self.holders[resource].get(tx) == mode

    def holds(self, tx, resource, mode):
        """True only for a currently *granted* lock (waiting requests do not count)."""
        held = self.holders[resource].get(tx)
        if held is None:
            return False
        return held == "X" or (mode == "S" and held == "S")

    def _drain(self, resource):
        """Grant from the FIFO head while the head has no conflicting holder.

        A run of mutually compatible S requests at the head is granted together;
        a waiting X stops the queue so later S requests cannot jump over it.
        """
        queue = self.waiters[resource]
        while queue:
            tx, mode = queue[0]
            if self.states.get(tx) != ACTIVE:
                queue.pop(0)
                continue
            if self._holder_blockers(tx, resource, mode):
                break
            queue.pop(0)
            self.holders[resource][tx] = mode

    # ------------------------------------------------------------------ #
    # cancellation / release
    # ------------------------------------------------------------------ #
    def cancel_pending(self, tx):
        """Remove only not-yet-granted requests; granted locks are untouched.

        A cancelled request may open the head of a queue, so every affected
        resource is drained afterwards.
        """
        self._tx_name(tx)
        if tx not in self.states:
            raise ValueError(f"unknown transaction {tx!r}")
        changed = []
        for resource, queue in self.waiters.items():
            if any(w == tx for w, _ in queue):
                self.waiters[resource] = [item for item in queue if item[0] != tx]
                changed.append(resource)
        for resource in changed:
            self._drain(resource)

    def release_all(self, tx):
        """Client-side release. Bound logical transactions must finish via
        commit/rollback instead of bypassing the Store this way."""
        self._tx_name(tx)
        if tx not in self.states:
            raise ValueError(f"unknown transaction {tx!r}")
        if tx in self._bound and self.states.get(tx) != ENDED:
            raise ValueError(
                f"transaction {tx!r} is a logical Store transaction; "
                "release happens through commit or rollback")
        self._release_locks(tx)

    def _release_locks(self, tx):
        """Internal release used by every terminal path (commit/rollback/abort)."""
        for resource in list(self.waiters):
            self.waiters[resource] = [item for item in self.waiters[resource]
                                      if item[0] != tx]
        changed = []
        for resource, map_ in list(self.holders.items()):
            if tx in map_:
                del map_[tx]
                changed.append(resource)
        for resource in changed:
            self._drain(resource)

    def terminate(self, tx):
        """Terminal barrier: mark ENDED and release locks, waking successors."""
        self._tx_name(tx)
        self._release_locks(tx)
        self.states[tx] = ENDED

    # ------------------------------------------------------------------ #
    # introspection
    # ------------------------------------------------------------------ #
    def snapshot(self):
        return {
            "states": dict(sorted(self.states.items())),
            "holders": {r: dict(sorted(v.items()))
                        for r, v in sorted(self.holders.items()) if v},
            "waiters": {r: [list(item) for item in v]
                        for r, v in sorted(self.waiters.items()) if v},
        }

    def wait_graph(self):
        graph = {tx: set() for tx, state in self.states.items() if state == ACTIVE}
        for resource, queue in self.waiters.items():
            for i, (tx, mode) in enumerate(queue):
                if self.states.get(tx) != ACTIVE:
                    continue
                edges = graph.setdefault(tx, set())
                # 1. conflicting granted holders
                edges.update(self._holder_blockers(tx, resource, mode))
                # 2. fair-queue predecessors that actually stop this request:
                #    an X conflicts with every predecessor; an S only with an X.
                for pred, pred_mode in queue[:i]:
                    if self.states.get(pred) != ACTIVE:
                        continue
                    if self._conflict(mode, pred_mode):
                        edges.add(pred)
        return {tx: sorted(edges) for tx, edges in sorted(graph.items())}

    def cycle_members(self):
        """All transactions lying on a real directed cycle (Tarjan SCC)."""
        graph = self.wait_graph()
        index = {}
        lowlink = {}
        on_stack = set()
        stack = []
        counter = [0]
        cyclic = set()

        def strongconnect(v):
            index[v] = lowlink[v] = counter[0]
            counter[0] += 1
            stack.append(v)
            on_stack.add(v)
            for w in graph.get(v, ()):  # edges only point at active txns
                if w not in index:
                    strongconnect(w)
                    lowlink[v] = min(lowlink[v], lowlink[w])
                elif w in on_stack:
                    lowlink[v] = min(lowlink[v], index[w])
            if lowlink[v] == index[v]:
                component = []
                while True:
                    w = stack.pop()
                    on_stack.discard(w)
                    component.append(w)
                    if w == v:
                        break
                if len(component) > 1 or v in graph.get(v, ()):
                    cyclic.update(component)

        for v in graph:
            if v not in index:
                strongconnect(v)
        return sorted(cyclic)

    # ------------------------------------------------------------------ #
    # deadlock resolution
    # ------------------------------------------------------------------ #
    def resolve_one(self, store=None):
        """Break exactly one real cycle per call.

        Selects the youngest victim (highest age; ties broken by the
        lexicographically larger transaction id). Pending requests are removed
        immediately; granted locks stay until the Store reports undo complete.
        Returns the victim, or None when no cycle exists.
        """
        members = self.cycle_members()
        if not members:
            return None
        victim = max(members, key=lambda tx: (self.ages[tx], tx))
        self.states[victim] = ABORTING
        # Unblock everyone behind the victim's queued requests, but keep grants.
        for resource in list(self.waiters):
            self.waiters[resource] = [item for item in self.waiters[resource]
                                      if item[0] != victim]
        for resource in list(self.waiters):
            self._drain(resource)
        if store is not None:
            # Store flips its own SQL state to ROLLING_BACK; locks remain held.
            store.begin_abort(victim)
        else:
            # Pure lock-only victim: no business rows to undo -> release now.
            self.terminate(victim)
        return victim
