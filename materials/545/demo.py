import json
from pathlib import Path
import tempfile
from schema import Model
from datastore import Datastore


if __name__ == "__main__":
    model = Model((Path(__file__).resolve().parent / "fabric.yang").read_text())
    initial = {"fabric":{"enabled":True,"max_rate":50,"port":[{"name":"p0","rate":5}]}}
    with tempfile.TemporaryDirectory() as root:
        store = Datastore(Path(root)/"config.sqlite",model,initial)
        store.replace_candidate("S1",{"fabric":{"port":[{"name":"p1"}]}},0)
        tentative = store.confirmed_commit("S1","C1",0,10)
        after = store.read_running(10)
        print(json.dumps({"initial":initial,"tentative":tentative,"after_expiry":after,
                          "invalid_leafref_accepted":model.normalize({"fabric":{"port":[{"name":"p1","peer":"missing"}]}})},indent=2))
        store.close()
