import json
from pathlib import Path
from clock_map import Clock
from access_units import Assembler
root=Path(__file__).parent/'fixtures';trace=json.loads((root/'trace.json').read_text())
clock=Clock([(root/p).read_bytes() for p in trace['reports']])
for case in ('single','fu_wrap'):
    print(json.dumps({case:Assembler().build([(root/p).read_bytes() for p in trace[case]],clock,trace['first_sequence'][case])}))
