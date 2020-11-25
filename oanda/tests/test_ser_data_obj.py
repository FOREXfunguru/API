import pytest
import pdb
import glob
import os
import datetime

from oanda.connect import Connect
from oanda.ser_data_obj import ser_data_obj

@pytest.fixture
def clean_tmp():
    yield
    print("Cleanup files")
    files = glob.glob(os.getenv('DATADIR')+"/out.data")
    for f in files:
        os.remove(f)

@pytest.fixture
def serialize_object():
    conn = Connect(instrument='AUD_USD',
                    granularity='D')

    conn.query(start='2015-01-25T22:00:00',
               count=10, outfile=os.getenv('DATADIR')+"/out.data")

def test_slice(serialize_object, clean_tmp):
    '''
    test of function 'slice'
    '''
    sdata_obj = ser_data_obj(ifile=os.getenv('DATADIR')+"/out.data")
    start = datetime.datetime(2015, 1, 29, 22, 0, 0)
    end = datetime.datetime(2015, 2, 1, 22, 0, 0)

    new_data = sdata_obj.slice(start=start, end=end)
    assert len(new_data['candles']) == 2

def test_slice_with_count(serialize_object, clean_tmp):
    '''
    test of function 'slice' and a number of candles to retrieve
    via the 'count' parameter
    '''
    sdata_obj = ser_data_obj(ifile=os.getenv('DATADIR')+"/out.data")
    start = datetime.datetime(2015, 1, 29, 22, 0, 0)

    new_data = sdata_obj.slice(start=start, count=3)
    assert len(new_data['candles']) == 3

def test_slice_p_start(serialize_object, clean_tmp):
    '''
    Test 'slice' function with a 'start' parameter that
    does not exist in the serialized data
    '''
    sdata_obj = ser_data_obj(ifile=os.getenv('DATADIR')+"/out.data")
    start = datetime.datetime(2010, 1, 29, 22, 0, 0)
    end = datetime.datetime(2015, 2, 1, 22, 0, 0)
    new_data = sdata_obj.slice(start=start, end=end)
    assert new_data['candles'][0]['time'] == "2015-01-25T22:00:00.000000Z"
    assert new_data['candles'][-1]['time'] == "2015-02-01T22:00:00.000000Z"




