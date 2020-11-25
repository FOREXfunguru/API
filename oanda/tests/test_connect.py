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
        granularity='D')
    return conn

@pytest.fixture
def clean_tmp():
    yield
    print("Cleanup files")
    os.remove(os.getenv('DATADIR')+"/ser.dmp")

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
    log = logging.getLogger('test_query_ser')
    log.debug('Test for \'query\' function with a non-valid instrument')
    conn = Connect(
        instrument="AUD_MOCK",
        granularity='D')

    respl = conn.query(start='2018-11-12T10:00:00',
                       end='2018-11-14T11:00:00')

    assert respl == 400

@pytest.mark.parametrize("i,g,s,e", [('GBP_NZD', 'D', '2018-11-23T22:00:00', '2019-01-02T22:00:00'),
                                     ('GBP_AUD', 'D', '2002-11-23T22:00:00', '2007-01-02T22:00:00'),
                                     ('EUR_NZD', 'D', '2002-11-23T22:00:00', '2007-01-02T22:00:00'),
                                     ('AUD_USD', 'D', '2015-01-25T22:00:00', '2015-01-26T22:00:00'),
                                     ('AUD_USD', 'D', '2018-11-16T22:00:00', '2018-11-20T22:00:00'),
                                     ('AUD_USD', 'H12', '2018-11-12T10:00:00', '2018-11-14T10:00:00'),
                                     # End date falling in the daylight savings discrepancy(US/EU) period
                                     ('AUD_USD', 'D', '2018-03-26T22:00:00', '2018-03-29T22:00:00'),
                                     # End date falling in Saturday at 21h
                                     ('AUD_USD', 'D', '2018-05-21T21:00:00', '2018-05-26T21:00:00'),
                                     # Start and End data fall on closed market
                                     ('AUD_USD', 'D', '2018-04-27T21:00:00', '2018-04-28T21:00:00'),
                                     # Start date before the start of historical record
                                     ('AUD_USD', 'H12', '2000-11-21T22:00:00', '2002-06-15T22:00:00')])
def test_m_queries(i, g, s, e):
    log = logging.getLogger('test_query_ser')
    log.debug('Test for \'query\' function with a mix of instruments and different datetimes')

    conn = Connect(
        instrument=i,
        granularity=g)

    respl = conn.query(start=s,
                       end=e)
