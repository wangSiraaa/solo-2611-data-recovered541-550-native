"""Small public offline YANG configuration normalizer; not all constraints work."""
from copy import deepcopy
import hashlib
from pyang import context, repository, error


class Model:
    def __init__(self, yang_text):
        self.digest = hashlib.sha256(yang_text.encode()).hexdigest()
        ctx = context.Context(repository.FileRepository("."))
        self.module = ctx.add_module("model.yang", yang_text)
        ctx.validate()
        failures = [(str(pos), code) for pos, code, args in ctx.errors if error.is_error(error.err_level(code))]
        if self.module is None or failures:
            raise ValueError("invalid YANG: " + repr(failures))

    @staticmethod
    def _default(statement):
        item = statement.search_one("default")
        if item is None:
            return None, False
        name = statement.search_one("type").arg
        value = item.arg
        if name == "boolean":
            value = value == "true"
        elif name == "uint16":
            value = int(value)
        return value, True

    def _object(self, declaration, value):
        if not isinstance(value, dict):
            raise ValueError("object expected")
        children = {s.arg:s for s in declaration.i_children}
        if set(value) - set(children):
            raise ValueError("unknown data node")
        result = {}
        for name, statement in children.items():
            if name not in value:
                default, present = self._default(statement) if statement.keyword == "leaf" else (None, False)
                if present:
                    # Current normalizer materializes defaults even when their when is false.
                    result[name] = default
                continue
            item = value[name]
            if statement.keyword == "container":
                result[name] = self._object(statement, item)
            elif statement.keyword == "list":
                if not isinstance(item, list):
                    raise ValueError("list expected")
                result[name] = [self._object(statement, entry) for entry in item]
                key = statement.search_one("key").arg
                keys = [entry[key] for entry in result[name]]
                if len(set(keys)) != len(keys):
                    raise ValueError("duplicate list key")
            elif statement.keyword == "leaf":
                kind = statement.search_one("type").arg
                if kind == "uint16" and (type(item) is not int or not 0 <= item <= 65535):
                    raise ValueError("uint16 expected")
                if kind == "boolean" and type(item) is not bool:
                    raise ValueError("boolean expected")
                if kind in ("string", "leafref") and (not isinstance(item, str) or not item):
                    raise ValueError("nonempty string expected")
                # A leafref currently gets only the scalar string check.
                result[name] = deepcopy(item)
        return result

    def normalize(self, document):
        return self._object(self.module, document)
