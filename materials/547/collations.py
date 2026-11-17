"""Small collation catalogue shared by import tools and the document store."""
import unicodedata


def key_for(text, definition):
    value = unicodedata.normalize('NFD', text)
    if definition['case_insensitive']:
        value = value.casefold()
    if not definition['accent_sensitive']:
        value = ''.join(ch for ch in value if unicodedata.category(ch) != 'Mn')
    return unicodedata.normalize('NFC', value)


class Handle:
    def __init__(self, name, revision, definition):
        self.name, self.revision, self.definition = name, revision, definition

    def key(self, text):
        return key_for(text, self.definition)

    def compare(self, left, right):
        a, b = self.key(left), self.key(right)
        return (a > b) - (a < b)


class Registry:
    def __init__(self):
        self.rules = {}
        self.revisions = {}

    def define(self, name, *, case_insensitive, accent_sensitive):
        definition = self.rules.setdefault(name, {})
        definition.update(case_insensitive=case_insensitive,
                          accent_sensitive=accent_sensitive)
        self.revisions[name] = self.revisions.get(name, 0) + 1
        return self.snapshot(name)

    def snapshot(self, name):
        return Handle(name, self.revisions[name], self.rules[name])
