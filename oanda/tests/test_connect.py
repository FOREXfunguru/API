import pytest
import logging
import pdb
import os

from oanda.connect import Connect

@pytest.fixture
def conn_o():
    log = logging.getLogger('connect_o')
    log.debug('Create a Connect object')

    # creating a Connect object
    conn = Connect(
        instrument="AUD_USD",
        granularity='D',
        settingf='../data/settings.ini')

    return conn

@pytest.fixture
def clean_tmp():
    yield
    print("Cleanup files")
    os.remove("../data/ser.dmp")

def test_query_s_e(conn_o):
    log = logging.getLogger('test_query_s_e')
    log.debug('Test for \'query\' function with a start and end datetimes')
    res = conn_o.query('2018-11-16T22:00:00', '2018-11-20T22:00:00')
    assert res['instrument'] == 'AUD_USD'
    assert res['granularity'] == 'D'
    assert len(res['candles']) == 3

def test_query_c(conn_o):
    log = logging.getLogger('test_query_c')
    log.debug('Test for \'query\' function with a start and count parameters')
    res = conn_o.query('2018-11-16T22:00:00', count=1)
    assert res['instrument'] == 'AUD_USD'
    assert res['granularity'] == 'D'
    assert len(res['candles']) == 1

def test_query_ser(conn_o, clean_tmp):
    log = logging.getLogger('test_query_ser')
    log.debug('Test for \'query\' function and serializing returned data')
    conn_o.query('2018-11-16T22:00:00', count=1, outfile="../data/ser.dmp")

def test_query_e():
    '''
    Test a simple query to Oanda's REST API using a non-valid pair
    '''

    # creating a Connect object
    conn = Connect(
        instrument="AUD_MOCK",
        granularity='D',
        settingf='../data/settings.ini')

    respl = conn.query(start='2018-11-12T10:00:00',
                       end='2018-11-14T11:00:00')

    assert respl == 400
