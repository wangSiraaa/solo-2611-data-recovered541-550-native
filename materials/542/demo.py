import json,tempfile
from pathlib import Path
from vrps import Directory
from rib import Rib

with tempfile.TemporaryDirectory() as tmp:
    root=Path(tmp); vrps=Directory(root/'vrps.sqlite'); rib=Rib(root/'rib.sqlite')
    vrps.begin('cache-A',reset=True);vrps.record('announce','192.0.2.0/24',25,65001);vrps.end(1)
    rib.announce('peer-A','192.0.2.0/25',7,65001)
    print(json.dumps({'normal':rib.facts(vrps)}))
    rib.announce('peer-A','192.0.2.0/25',8,65002)
    print(json.dumps({'two_announcements':rib.rows(),'required_path_ids':[7,8]}))
    vrps.begin('cache-A',base_serial=1);vrps.record('withdraw','192.0.2.0/24',25,65001)
    print(json.dumps({'unfinished_directory':vrps.snapshot(),'required_still_published_serial':1}))
    rib.close();vrps.close()
