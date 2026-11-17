import json
from pathlib import Path
from entailment import Entailment,TYPE
from constraints import Validator
cases=json.loads((Path(__file__).parent/'graphs.json').read_text())
e=Entailment();v=Validator()
normal=e.materialize(cases['normal'])
print(json.dumps({'normal_discovery':normal.subjects(TYPE,'ex:Meter'),'normal_quality':v.validate(normal,[{'name':'MeterShape','targetClass':'ex:Meter','properties':[{'path':['ex:serial'],'minCount':1}]}])}))
chain=e.materialize(cases['chain']);print(json.dumps({'chain_Asset_clients':chain.subjects(TYPE,'ex:Asset'),'required':['ex:a']}))
diamond=e.materialize(cases['diamond']);print(json.dumps({'diamond_quality':v.validate(diamond,[{'name':'MeterShape','targetClass':'ex:Meter','properties':[{'path':['ex:p','ex:q'],'maxCount':1}]}]),'required':[]}))
