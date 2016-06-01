import uuid
import pytest
from metadatastore.mds import MDS
from metadatastore.utils import create_test_database


@pytest.fixture(params=[0, 1, 'cmds'], scope='function')
def mds_all(request):
    '''Provide a function level scoped FileStore instance talking to
    temporary database on localhost:27017 with both v0 and v1.

    '''
    db_template = "mds_testing_disposable_{}".format(str(uuid.uuid4()))
    ver = request.param
    obj_api = True
    if ver == 'cmds':
        ver = 1
        obj_api = False
    test_conf = create_test_database('localhost', db_template=db_template,
                                     version=ver)
    mds = MDS(test_conf, ver)

    def delete_dm():
        print("DROPPING DB")
        mds._connection.drop_database(test_conf['database'])

    request.addfinalizer(delete_dm)

    if obj_api:
        return mds
    else:
        import metadatastore.commands as mdsc
        mdsc._DB_SINGLETON = mds
        return mdsc
