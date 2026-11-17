"""Finite RDF entailment prototype used by catalog discovery clients."""
import copy
import hashlib
import json
TYPE='rdf:type'
SUBCLASS='rdfs:subClassOf'
SUBPROPERTY='rdfs:subPropertyOf'
DOMAIN='rdfs:domain'
RANGE='rdfs:range'

def freeze_term(value):
    return value if isinstance(value,str) else ('L',value['lexical'],value['datatype'])
def term_json(value):
    return value if isinstance(value,str) else {'lexical':value[1],'datatype':value[2]}
def triple_key(triple):
    return json.dumps([triple[0],triple[1],term_json(triple[2])],ensure_ascii=False,sort_keys=True)

class Snapshot:
    def __init__(self,triples):
        self._triples=tuple(sorted(set((s,p,freeze_term(o)) for s,p,o in triples),key=triple_key))
    def triples(self):
        return tuple(self._triples)
    def objects(self,subject,predicate):
        return [o for s,p,o in self._triples if s==subject and p==predicate]
    def subjects(self,predicate,obj):
        return sorted({s for s,p,o in self._triples if p==predicate and o==obj})
    def export(self):
        return [[s,p,term_json(o)] for s,p,o in self._triples]
    def digest(self):
        return hashlib.sha256(json.dumps(self.export(),ensure_ascii=False,sort_keys=True,separators=(',',':')).encode()).hexdigest()

class Entailment:
    def materialize(self,triples):
        original=Snapshot(triples).triples()
        result=set(original)
        # The prototype applies each declared implication once to original facts.
        for s,p,o in original:
            for a,rule,b in original:
                if p==TYPE and rule==SUBCLASS and o==a:
                    result.add((s,TYPE,b))
                if rule==SUBPROPERTY and p==a:
                    result.add((s,b,o))
                if rule==DOMAIN and p==a:
                    result.add((s,TYPE,b))
                if rule==RANGE and p==a and isinstance(o,str):
                    result.add((o,TYPE,b))
        return Snapshot([(s,p,term_json(o)) for s,p,o in result])
