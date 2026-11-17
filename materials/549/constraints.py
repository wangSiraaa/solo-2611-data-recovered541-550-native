"""Graph-node quality validation, also accepts externally materialized graphs."""
from entailment import TYPE,term_json

class Validator:
    def values(self,view,focus,path):
        current=[focus]
        for step in path:
            following=[]
            for node in current:
                if not isinstance(node,str):
                    continue
                if isinstance(step,dict):
                    following.extend(view.subjects(step['inverse'],node))
                else:
                    following.extend(view.objects(node,step))
            current=following
        return current

    def validate(self,view,shapes):
        self.last_input_digest=view.digest()
        rows=[]
        for shape in shapes:
            for focus in view.subjects(TYPE,shape['targetClass']):
                for index,prop in enumerate(shape.get('properties',[])):
                    values=self.values(view,focus,prop['path'])
                    common={'focus':focus,'shape':shape['name'],'property':index}
                    if len(values)<prop.get('minCount',0):
                        rows.append(dict(common,constraint='minCount',count=len(values)))
                    if 'maxCount' in prop and len(values)>prop['maxCount']:
                        rows.append(dict(common,constraint='maxCount',count=len(values)))
                    for value in values:
                        if 'class' in prop and (not isinstance(value,str) or prop['class'] not in view.objects(value,TYPE)):
                            rows.append(dict(common,constraint='class',value=term_json(value)))
                        if 'datatype' in prop and (isinstance(value,str) or value[2]!=prop['datatype']):
                            rows.append(dict(common,constraint='datatype',value=term_json(value)))
        return rows
