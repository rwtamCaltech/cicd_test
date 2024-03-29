from quakes2aws_datastore.logging import logger  # noqa:E402
import time
from contexttimer import Timer
from datetime import datetime
from PickRun import PickRun
import logging

# Initialize Logger
LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

#We are testing a unit test area:
# from flask import Flask
# app = Flask(__name__)

# @app.route('/<random_string>')
def returnBackwardsString(random_string):
    """Reverse and return the provided URI"""
    LOGGER.info('Received a message: %s', random_string)
    return "".join(reversed(random_string))


class PickRunner:
    """
    Run the picker in a loop.
    """
    def __init__(self, starttime=None, count=None, no_sleep=False, live=True, mark_as_processed=True,querymech="Base",querypicks="Base",test=False):
        """
        :param starttime float: start data going forward from this SampleSet starttime
        :param count int: do this many iterations, then stop
        :param no_sleep bool: don't sleep between iterations
        """

        #3/28/23 RT add -->added the querymechanism here
        self.querymech=querymech

        #5/8/23 added
        self.querypicks=querypicks

        #We will call a function to get the maximum starttime at the moment
        max_starttime, record_maxtime_seismicquery_time_elapsed=self.__find_maxstarttime()

        #gbytes_scanned_tot,gbytes_metered_tot,cost_for_query

        logger.info('max_timestream.query', query_time_elapsed=record_maxtime_seismicquery_time_elapsed,max_starttime=max_starttime)
        logger.info('picker.boot')

        self.binsize = 30 
        self.PICKER_DELAY_SECONDS=2 #use default here, how far behind we pull data for picks, prev 90
        self.PICKER_SAMPLING_RATE_FILTER=100.0 #only do picks on channels with this sampling rate
        # PICKER_DELAY_SECONDS = env.int('PICKER_DELAY_SECONDS', default=90)

        starttime = time.time() - self.PICKER_DELAY_SECONDS - self.binsize

        self.__starttime = None
        if starttime:
            self.__starttime = starttime
        self.__count = count if count is not None else None
        self.no_sleep = no_sleep
        self.mark_as_processed = mark_as_processed
        self.samprate = self.PICKER_SAMPLING_RATE_FILTER
        self.test=test

        #Introduced a stub value for comparison purposes; it starts at the end of the DB
        # self.__stub=self.state.endtime #thought this could be None at first
        #1/11/23 RT STUB update: we now take this to be the max_starttime
        self.__stub=max_starttime #likely None at first, since there is no data in the DB (it has been cleared out with the expiry)


    #3/15/23 Will create a new function to find the maximum time. I will reference QueryExample too find the max start time 
    #fn here and get the maximum time from there.
    def __find_maxstarttime(self):
        with Timer() as record_maxtime_seismicquery_time:
            maxtime_found=self.querymech.get_max_timestamp_query()

            if not maxtime_found: #if the query was empty, then we 
                self.max_starttime=None #Set the maximum value to none here if there is nothing in the DB
            else: 
                self.max_starttime=int(maxtime_found) #maxtime_found is of type float, we cast it to int here

        record_maxtime_seismicquery_time_elapsed=round(record_maxtime_seismicquery_time.elapsed,3)
        return self.max_starttime, record_maxtime_seismicquery_time_elapsed #We will update the max_starttime each time we run this, so we ca reference it

    #3/15/23 find minimum time. 
    def __find_minstarttime(self):
        with Timer() as record_mintime_seismicquery_time:
            mintime_found=self.querymech.get_min_timestamp_query()

            if not mintime_found: #if the query was empty, then we 
                self.min_starttime=None #Set the maximum value to none here if there is nothing in the DB
            else: 
                self.min_starttime=int(mintime_found) #maxtime_found is of type float, we cast it to int here

        record_mintime_seismicquery_time_elapsed=round(record_mintime_seismicquery_time.elapsed,3)
        return self.min_starttime, record_mintime_seismicquery_time_elapsed #We will update the min_starttime each time we run this, so we can reference it

    def __guess_starttime(self):
        starttime = time.time() - self.PICKER_DELAY_SECONDS - self.binsize
        return starttime

    #This container will be invoked only when we start funneling in data, so as long as we funnel data, eventually, we will get past that this problem,
    #if this problem even exists.
    def __wait_for_enough_data(self):
        """
        Let ``settings.PICKER_DELAY_SECONDS`` seconds accumulate in the staging database before doing picks. This allows
        at least some late arriving packets to get here before we try to pick them.
        """
        # while self.state.is_empty:
        while self.__stub==None:
            logger.info('pick.starttime.no-data', sleep=self.binsize)
            time.sleep(self.binsize)
        wanted_starttime = self.__guess_starttime() #this will get the current time, so if our current time is somehow less than our sample, we get the bellow
        
        #Get the minimum starttime here if needed
        min_starttime, _=self.__find_minstarttime()
        oldest_sample = min_starttime
        # oldest_sample = self.state.starttime

        while wanted_starttime < oldest_sample:
            logger.info(
                'picker.starttime.not-enough-data',
                wanted_starttime=wanted_starttime,
                oldest_sample=oldest_sample,
                delay_seconds=self.PICKER_DELAY_SECONDS,
                binsize=self.binsize,
                sleep=self.binsize
            )
            time.sleep(self.binsize)
            wanted_starttime = self.__guess_starttime()

            #Get the minimum starttime here if needed
            min_starttime, _=self.__find_minstarttime()
            oldest_sample = min_starttime
            # oldest_sample = self.state.starttime

    @property
    def starttime(self):
        if not self.__starttime: 
            #I don't think it ever starts here
            self.__wait_for_enough_data()
            starttime  = self.__guess_starttime()
        else:
            starttime = self.__starttime
            self.__starttime += self.binsize

        logger.info('picker.starttime', __startime=self.__starttime)
        return starttime

    def done(self):
        if self.__count is None:
            return False
        if self.__count == 0:
            return True
        self.__count -= 1
        return False

    def run(self, test=False):
        present_time='N/A'

        #No need to trim the DB, we are relying on the autodeletes of Timestream. 
        # #Explore trimming the database from here
        # counter_trimDB=0 

        #4/12/23 - at the beginning, expire ALL waveform data, so we have a clean slate to work with
        string_memBeforeClear=self.querymech.get_memory_usage()
        self.querymech.expire_all_data()

        #5/8/23 - added expiring all the pick data here too, below memory check checks ALL memory in Redis DB
        self.querypicks.expire_all_picks()
        string_memAfterClear=self.querymech.get_memory_usage()

        logger.info(
            'beginningExpireAllData.memorycheck.results',
            memory_atBegin=string_memBeforeClear,
            memory_afterExpir=string_memAfterClear
        )

        #4/12/23 - also set a default max_starttime when we first run this
        max_starttime=-99999 #some impossible number to reach at first
        start_time_used=-99999 #set this to some impossible number to reach at first

        while True:
            #4/12/23 if we get to a case where we cancel the stream of data we are running, then it is likely the max_starttime would equal
            #the previous max_starttime
            oldermax_starttime=max_starttime #this refers to the latestTS
            olderstarttime_used=start_time_used #refers to the older currentTS

            #One element is to make sure we have caught up to the DB (taken from the job fn)
            start_time_used=self.__starttime
            now = datetime.now() # current date and time
            starting_format = now.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-7]
            present_time=starting_format

            #3/15/23 RT update; Chris had a PostGRES command to get the maximum time of the data ingested in the DB as well. We need to do this within Postgres.
            # max_starttime, record_maxtime_seismicquery_time_elapsed,gbytes_scanned_tot_max,gbytes_metered_tot_max,cost_for_query_max=self.__find_maxstarttime()
            max_starttime, record_maxtime_seismicquery_time_elapsed=self.__find_maxstarttime()
            tot_query_elapsed=record_maxtime_seismicquery_time_elapsed

            #Recall that start_time_used is starttime = time.time() - self.PICKER_DELAY_SECONDS - self.binsize, so based off the current time we are running.
            #However, we only increment this start time if we get to the beginningOfNewJob, so we need to store new data continuously.
            logger.info('monitor_initial', currentTS=start_time_used, latestTS=max_starttime,stubValue=self.__stub, query_time_elapsed=tot_query_elapsed)

            # if start_time_used<=self.state.endtime or self.state.endtime != self.__stub:


            #4/12/23 I want to add a conditional for realtime that leads to idling if we are not getting any new data (IE we stop our stream source)
            if max_starttime==oldermax_starttime and start_time_used==olderstarttime_used:
                logger.info(
                    'caughtUpToDBSoIdle',
                    latestTS=str(self.max_starttime),
                    presentTime=starting_format,
                    binSize=self.binsize,
                    sampRate=self.samprate,
                    currentTS=str(start_time_used)
                )

                #I will test a new condition to sleep for 30 seconds if we are idling (just so we clean out our cloudwatch logs in idle cases)
                time.sleep(30)
            elif start_time_used<=max_starttime or max_starttime != self.__stub:
                logger.info(
                    'beginningOfNewJob',
                    presentTime=present_time,
                    currentTS=str(start_time_used),
                    latestTS=str(max_starttime),
                    binSize=self.binsize
                )

                #RT 3/29/23 unit testing add: Adding the querymechanism within here. self.querymech
                run_time = PickRun(
                    self.max_starttime,
                    self.binsize,
                    self.samprate,
                    present_time,
                    mark_as_processed=self.mark_as_processed,
                    querymech=self.querymech,
                    querypick=self.querypicks
                ).run(test=self.test)

                #Then
                self.__stub=self.max_starttime
                incremented_time=self.starttime #value won't be used, but we NEED THIS to increment by binsize each time.

                if not self.no_sleep: #we sleep if we complete before 30 seconds
                    if run_time < self.binsize:
                        sleep_time = self.binsize - run_time
                        logger.info('picker.sleep', sleep_time=sleep_time)
                        time.sleep(sleep_time)
                    else:
                        logger.warning('picker.overrun', run_time=run_time)
            else: #too far ahead, do not write anything
                logger.info(
                    'caughtUpToDBSoIdle',
                    latestTS=str(self.max_starttime),
                    presentTime=starting_format,
                    binSize=self.binsize,
                    sampRate=self.samprate,
                    currentTS=str(start_time_used)
                )

                #I will test a new condition to sleep for 30 seconds if we are idling (just so we clean out our cloudwatch logs in idle cases)
                time.sleep(30)
                
                '''
                4/11/23: ACTUALLY comment out the below, I wonder if constant expiration of data might lead to some of the problems seen.
                That may be why our latestTS might be stuck all the time.
                '''
                # #4/11/23 if we ever get an idling case (for instance, switching from modern to replay data within seconds of each other), we
                # #can make sure to clear the Redis DB in this way, and expire the data
                # string_memBeforeClear=self.querymech.get_memory_usage()

                # all_query_results=self.querymech.expire()
                # string_memAfterClear=self.querymech.get_memory_usage()

                # logger.info(
                #     'memorycheck.results',
                #     memory_atBegin=string_memBeforeClear,
                #     memory_afterExpir=string_memAfterClear
                # )

                # #^^The expectation is that after one iteration of "caughtUpToDBSoIdle", it will error out and try to relaunch the ECS instance many times instead.
                # #If there is no data going through to it anymore.
