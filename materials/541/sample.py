from pathlib import Path
from repository import Repository
from catalog import Catalog


def dc(value):
    import xml.sax.saxutils
    return '<oai_dc:dc xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/" xmlns:dc="http://purl.org/dc/elements/1.1/"><dc:title>'+xml.sax.saxutils.escape(value)+'</dc:title></oai_dc:dc>'


if __name__=='__main__':
    import json,tempfile
    with tempfile.TemporaryDirectory() as directory:
        root=Path(directory); repo=Repository(root/'repo.sqlite'); cat=Catalog(root/'catalog.sqlite')
        repo.put('record-a','oai_dc',10,['reports'],dc('First report'))
        print(json.dumps({'initial':cat.sync(repo,'source-one','oai_dc','reports')},ensure_ascii=False))
        repo.put('record-b','oai_dc',10,['reports'],dc('Another report in the same second'))
        print(json.dumps({'same_second_update':cat.sync(repo,'source-one','oai_dc','reports')},ensure_ascii=False))
        cat.close();repo.close()
