'''
@date: 22/11/2020
@author: Ernesto Lowy
@email: ernestolowy@gmail.com
'''
import os
import pdb
import datetime
import time
import logging
import requests
import re
import pandas as pd
import json

from oanda.config import CONFIG

# create logger
o_logger = logging.getLogger(__name__)
o_logger.setLevel(logging.INFO)

class Connect(object):
    """
    Class representing a connection to the Oanda's REST API
    """

    def __init__(self, instrument, granularity):
        '''
        Constructor

        Class variables
        ---------------
        instrument: string
                    Trading pair. i.e. AUD_USD. Required
        granularity: string
                     Timeframe. i.e. D. Required
        '''
        self.instrument = instrument
        self.granularity = granularity

    def retry(cooloff=5, exc_type=None):
        '''
        Decorator for retrying connection and prevent TimeOut errors
        '''
        if not exc_type:
            exc_type = [requests.exceptions.ConnectionError]

        def real_decorator(function):
            def wrapper(*args, **kwargs):
                while True:
                    try:
                        return function(*args, **kwargs)
                    except Exception as e:
                        if e.__class__ in exc_type:
                            print("failed (?)")
                            time.sleep(cooloff)
                        else:
                            raise e

            return wrapper

        return real_decorator

    @retry()
    def query(self, start, end=None, count=None, outfile=None):
        '''
        Function to run a particular REST API query
        with a start and end datetimes

        Parameters
        ----------
        start: Datetime in isoformat
               Date and time for first candle. Required
        end:   Datetime in isoformat
               Date and time for last candle. Optional
        count: int
               If end is not defined, this controls the
               number of candles from the start
               that will be retrieved
        outfile: str
                 File to write serialized data returned
                 by the API

        Returns
        -------
        dict with candle data
        '''

        startObj = self.validate_datetime(start, self.granularity)
        start = startObj.isoformat()
        params = {}
        if end is not None and count is None:
            endObj = self.validate_datetime(end, self.granularity)
            min = datetime.timedelta(minutes=1)
            endObj = endObj + min
            end = endObj.isoformat()
            params['end'] = end
        elif count is not None:
            params['count'] = count
        elif end is None and count is None:
            raise Exception("You need to set at least the 'end' or the 'count' attribute")

        params['instrument'] = self.instrument
        params['granularity'] = self.granularity
        params['start'] = start
        try:
            resp = requests.get(url=CONFIG.get('settings', 'url'),
                                params=params)
            if resp.status_code != 200:
                raise Exception(resp.status_code)
            else:
                data = json.loads(resp.content.decode("utf-8"))
                if outfile is not None:
                    ser_data = json.dumps(data)
                    f = open(outfile, "w")
                    f.write(ser_data)
                    f.close()
                return data
        except Exception as err:
            # This means something went wrong.
            print("Something went wrong. url used was:\n{0}".format(resp.url))
            print("Error message was: {0}".format(err))
            return resp.status_code
        return resp.status_code

    def validate_datetime(self, datestr, granularity):
        '''
        Function to parse a string datetime to return a datetime object and to validate the datetime

        Parameters
        ----------
        datestr : string
                  String representing a date
        granularity : string
                      Timeframe
        '''
        # Generate a datetime object from string
        dateObj = None
        try:
            dateObj = pd.datetime.strptime(datestr, '%Y-%m-%dT%H:%M:%S')
        except ValueError:
            raise ValueError("Incorrect date format, should be %Y-%m-%dT%H:%M:%S")

        patt = re.compile("\dD")
        nhours = None
        delta = None
        if patt.match(granularity):
            raise Exception("{0} is not valid. Oanda rest service does not take it".format(granularity))
        elif granularity == "D":
            nhours=24
            delta = datetime.timedelta(hours=24)
        else:
            p1 = re.compile('^H')
            m1 = p1.match(granularity)
            if m1:
                nhours = int(granularity.replace('H', ''))
                delta = datetime.timedelta(hours=int(nhours))
            nmins = None
            p2 = re.compile('^M')
            m2 = p2.match(granularity)
            if m2:
                nmins = int(granularity.replace('M', ''))
                delta = datetime.timedelta(minutes=int(nmins))

        # increment dateObj by one period. This is necessary in order to query Oanda
        endObj = dateObj+delta

        # check if datestr returns a candle
        params = {}
        params['instrument'] = self.instrument
        params['granularity'] = self.granularity
        params['start'] = datestr
        params['end'] = endObj.isoformat()
        resp = requests.get(url=CONFIG.get('settings', 'url'),
                            params=params)
        # 204 code means 'no_content'
        if resp.status_code == 204:
            if CONFIG.getboolean('settings', 'roll') is True:
                dateObj = self.__roll_datetime(dateObj, granularity)
            else:
                raise Exception("Date {0} is not valid and falls on closed market".format(datestr))

        if nhours is not None:
            base= datetime.time(22, 00, 00)
            # generate a list with valid times. For example, if granularity is H12, then it will be 22 and 10
            valid_time = [(datetime.datetime.combine(datetime.date(1, 1, 1), base) +
                           datetime.timedelta(hours=x)).time() for x in range(0, 24, nhours)]

            # daylightime saving discrepancy
            base1 = datetime.time(21, 00, 00)
            valid_time1 = [(datetime.datetime.combine(datetime.date(1, 1, 1), base1) +
                            datetime.timedelta(hours=x)).time() for x in range(0, 24, nhours)]
        return dateObj

    def __roll_datetime(self, dateObj, granularity):
        '''
        Private function to roll the datetime, which falls on a closed market to the next period (set by granularity)
        with open market

        If dateObj falls before the start of the historical data record for self.instrument then roll to the start
        of the historical record

        Parameters
        ----------
        dateObj : datetime object

        Returns
        -------
        datetime object
                 Returns the rolled datetime object
        '''
        # check if dateObj is previous to the start of historical data for self.instrument
        if not CONFIG.has_option('pairs_start', self.instrument):
            raise Exception("Inexistent start of historical record info for {0}".format(self.instrument))

        start_hist_dtObj = self.try_parsing_date(CONFIG.get('pairs_start', self.instrument))
        if dateObj < start_hist_dtObj:
            rolledateObj = start_hist_dtObj
            o_logger.debug("Date precedes the start of the historical record.\n"
                           "Time was rolled from {0} to {1}".format(dateObj, rolledateObj))
            return rolledateObj

        delta = None
        if granularity == "D":
            delta = datetime.timedelta(hours=24)
        else:
            p1 = re.compile('^H')
            m1 = p1.match(granularity)
            if m1:
                nhours = int(granularity.replace('H', ''))
                delta = datetime.timedelta(hours=int(nhours))
            p2 = re.compile('^M')
            m2 = p2.match(granularity)
            if m2:
                nmins = int(granularity.replace('M', ''))
                delta = datetime.timedelta(minutes=int(nmins))

        resp_code = 204
        startObj = dateObj
        while resp_code == 204:
            startObj = startObj+delta
            endObj = startObj+delta
            #check if datestr returns a candle
            params = {}
            params['instrument'] = self.instrument
            params['granularity'] = self.granularity
            params['start'] = dateObj.isoformat()
            params['end'] = endObj.isoformat()

            resp = requests.get(url=CONFIG.get('settings', 'url'),
                                params=params)
            resp_code = resp.status_code
        o_logger.debug("Time was rolled from {0} to {1}".format(dateObj, startObj))
        return startObj

    def __validate_end(self, endObj):
        '''
        Private method to check that last candle time matches the 'end' time provided
        within params

        Parameters
        ---------
        endObj :   Datetime object

        Returns
        -------
        1 if it validates
        '''

        endFetched = pd.datetime.strptime(self.data['candles'][-1]['time'], '%Y-%m-%dT%H:%M:%S.%fZ')
        if endObj != endFetched:
            #check if discrepancy is not in the daylight savings period
            fetched_time = endFetched.time()
            passed_time = endObj.time()
            dateTimefetched = datetime.datetime.combine(datetime.date.today(), fetched_time)
            dateTimepassed = datetime.datetime.combine(datetime.date.today(), passed_time)
            dateTimeDifference = dateTimefetched - dateTimepassed
            dateTimeDifferenceInHours = dateTimeDifference.total_seconds() / 3600
            if endFetched.date() == endObj.date() and abs(dateTimeDifferenceInHours) <= 1:
                return 1
            else:
                raise Exception("Last candle time does not match the provided end time")
        else:
            return 1

    def print_url(self):
        '''
        Print url from requests module
        '''
        
        print("URL: %s" % self.resp.url)

    def try_parsing_date(self, text):
        '''
        Function to parse a string that can be formatted in
        different datetime formats

        :returns
        datetime object
        '''

        for fmt in ('%Y-%m-%dT%H:%M:%S', '%Y-%m-%d %H:%M:%S'):
            try:
                return datetime.datetime.strptime(text, fmt)
            except ValueError:
                pass
        raise ValueError('no valid date format found')

    def __repr__(self):
        return "connect"

    def __str__(self):
        out_str = ""
        for attr, value in self.__dict__.items():
            out_str += "%s:%s " % (attr, value)
        return out_str
