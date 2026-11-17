import json,tempfile
from pathlib import Path
from txn_directory import Directory
from bucket_reader import Reader
with tempfile.TemporaryDirectory() as tmp:
    root=Path(tmp);d=Directory(root/'tx.sqlite');t=d.begin();w=d.allocate(t,'measurements');d.finish(t,'committed')
    row={'current_write_id':w,'original_write_id':w,'bucket_id':0,'row_id':0,'operation_no':0,'value':7}
    (root/'one.jsonl').write_text(json.dumps(row)+'\n')
    manifest={'table':'measurements','bases':[],'deltas':[{'path':'one.jsonl','kind':'insert','statement_id':0,'min_write_id':w,'max_write_id':w}]}
    print(json.dumps({'normal':Reader(root).scan(d,'measurements',manifest)}))
    t2=d.begin();d.allocate(t2,'other');d.finish(t2,'committed')
    t3=d.begin();w3=d.allocate(t3,'measurements')
    print(json.dumps({'open_table_write_id':w3,'global_txn_id':t3,'snapshot':d.snapshot('measurements')}))
    d.close()
