"""Small runnable policy prototype; not PostgreSQL or a security sandbox."""
from copy import deepcopy


def evaluate(expr, row, actor):
    if isinstance(expr, bool) or expr is None:
        return expr
    op, *parts = expr
    if op == 'field':
        return row[parts[0]]
    if op == 'actor':
        return actor[parts[0]]
    if op == 'literal':
        return parts[0]
    values = [evaluate(part, row, actor) for part in parts]
    if op == 'eq':
        return None if None in values else values[0] == values[1]
    if op == 'lt':
        return None if None in values else values[0] < values[1]
    if op == 'is_null':
        return values[0] is None
    if op == 'not':
        return None if values[0] is None else not values[0]
    if op == 'and':
        return False if False in values else (None if None in values else True)
    if op == 'or':
        return True if True in values else (None if None in values else False)
    raise ValueError(op)


class Plan:
    def __init__(self, revision, actor, using, check):
        self.revision = revision
        self.actor = deepcopy(actor)
        self.program = {'revision': revision, 'using': deepcopy(using), 'check': deepcopy(check)}

    def allows(self, phase, row):
        return evaluate(self.program[phase], row, self.actor) is True


class PolicyCatalog:
    def __init__(self):
        self.revisions = []

    def publish(self, memberships, policies):
        self.revisions.append(deepcopy({'memberships': memberships, 'policies': policies}))
        return len(self.revisions)

    def compile(self, actor, command, revision=None):
        revision = len(self.revisions) if revision is None else revision
        data = self.revisions[revision-1]
        # The prototype selects one directly applicable rule, as its first caller needed.
        for rule in data['policies']:
            if rule['role'] in ('*', actor['role']) and rule['command'] in ('ALL', command):
                using = rule.get('using', True)
                return Plan(revision, actor, using, rule.get('check', using))
        return Plan(revision, actor, False, False)
