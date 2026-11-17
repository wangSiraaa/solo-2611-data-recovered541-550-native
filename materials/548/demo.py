from tempfile import TemporaryDirectory
from pathlib import Path
from policy import PolicyCatalog
from row_gateway import RowGateway


def eq(field, value):
    return ['eq', ['field', field], ['literal', value]]


if __name__ == '__main__':
    catalog = PolicyCatalog()
    catalog.publish({}, [{'name':'own','role':'analyst','command':'ALL','mode':'permissive','using':eq('tenant','A'),'check':eq('tenant','A')}])
    with TemporaryDirectory() as tmp:
        gateway = RowGateway(Path(tmp)/'facts.sqlite', catalog)
        gateway.seed([{'id':1,'tenant':'A','owner':'u','value':10}, {'id':2,'tenant':'B','owner':'v','value':20}])
        actor={'role':'analyst','tenant':'A','user':'u'}
        print('before', gateway.select(actor))
        print('update', gateway.update(actor,[{'id':1,'tenant':'B'}]))
        print('physical rows', [dict(row) for row in gateway.db.execute('SELECT * FROM facts ORDER BY id')])
        gateway.close()
