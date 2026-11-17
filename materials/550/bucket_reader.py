"""Read actual JSONL physical rows; preserves a simple one-insert baseline."""
import json
from pathlib import Path

class Reader:
    def __init__(self,root):self.root=Path(root)
    def scan(self,directory,table,manifest):
        snapshot=directory.snapshot(table)
        return self.scan_snapshot(snapshot,manifest)
    def scan_snapshot(self,snapshot,manifest):
        live={}
        for file in manifest['deltas']:
            with (self.root/file['path']).open() as stream:
                for line in stream:
                    row=json.loads(line);wid=row['current_write_id']
                    if wid>snapshot['high_write_id'] or wid in snapshot['invalid_write_ids']:continue
                    rid=(row['bucket_id'],row['row_id'])
                    if file['kind']=='insert':live[rid]=row
                    else:live.pop(rid,None)
        return sorted(live.values(),key=lambda r:(r['original_write_id'],r['bucket_id'],r['row_id']))
