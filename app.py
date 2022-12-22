# """
# Main application file:
# a simple Flask web application.  This application contains one endpoint that reverses and returns the requested URI
# """

# from flask import Flask
# app = Flask(__name__)

# @app.route('/<random_string>')
# def returnBackwardsString(random_string):
#     """Reverse and return the provided URI"""
#     return "".join(reversed(random_string))

# if __name__ == '__main__':
#     app.run(host='0.0.0.0', port=8080)



# """
# Main application file
# This is to break the unit test
# """
# from flask import Flask
# app = Flask(__name__)

# @app.route('/<random_string>')
# def returnBackwardsString(random_string):
#     """Reverse and return the provided URI"""
#     return "Breaking the unit test"

# if __name__ == '__main__':
#     app.run(host='0.0.0.0', port=8080)

"""
Main application file
Works with additional logging statements, test Dependabot
added secret keys
"""
from flask import Flask
import logging, time, os, sys, traceback
import pandas as pd
# import psutil  #already installed as part of python
app = Flask(__name__)
import django

#briefly commented
#Testing
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'quakes2aws_datastore.settings')
django.setup()

from django.utils import timezone
from quakes2aws_datastore.logging import logger  # noqa:E402
from quakes2aws_datastore.core.models import State
import quakes2aws_datastore.s3 as s3
from django.db import connection
from contexttimer import Timer
from timeout import timeout 
from datetime import datetime,timedelta
from zipfile import ZipFile
import zipfile

# Initialize Logger
LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

@app.route('/<random_string>')
def returnBackwardsString(random_string):
    """Reverse and return the provided URI"""
    LOGGER.info('Received a message: %s', random_string)
    return "".join(reversed(random_string))

#implemented a timeout decorator, we want our access to happen within 2 seconds, otherwise we "sense" something is wrong and get data through other means
#We saw that connection could take 40+ seconds; SETTING to 0.000005 works
@timeout(2)
def connect_db(connection):
    with connection.cursor() as cursor:
        cursor.execute('\conninfo') 
        row = cursor.fetchall() 
        cursor.close()
    return row

    # with connection.cursor() as cursor:
    #     #https://stackoverflow.com/questions/67678201/how-to-specify-timeout-for-a-query-using-django
    #     # cursor.execute("SET statement_timeout = 2;") #set statement timeout here, see if it can set the timeout to 2 seconds here
    #     cursor.execute('SELECT endtime FROM sample_set ORDER BY "id" DESC LIMIT 1;') 
    #     row = cursor.fetchall() 
    #     # cursor.close()
    
    # with connection.cursor() as cursorTwo:
    #     cursorTwo.execute("SELECT pg_size_pretty( pg_total_relation_size('sample_set') );")
    #     rowTwo= cursorTwo.fetchall() 
    #     # cursorTwo.close()

    # return row,rowTwo

class PickRun:
    """
    Run a single pick iteration.

    Ryan notes; took out fn: def mark_sets_as_processed(self, set_ids); don't think we use them.
    """
    def __init__(self, state, starttime, binsize, samprate, present_time,counter_trimDB,mark_as_processed=True):
        self.state = state
        self.starttime = starttime
        self.binsize = binsize
        self.samprate = samprate
        self.present_time=present_time
        self.counter_trimDB=counter_trimDB #This was used to check the sampleset key to make sure we don't overrun the primary key
        self.mark_as_processed = mark_as_processed

    #Took out a lot of DB type saves in the start and finish sections here. Don't save pick_run_stats or summary at start and finish, respectively.
    def __start(self):
        logger.info('picker.run.start', starttime=self.starttime, binsize=self.binsize)

    def __finish(self, state='done'):
        logger.info('picker.run.finish')

    def batch(self):
        """
        Load data from the database; 7/6/21 RT: we get the last minute of data from the DB here.
        :rtype: QuerySet of TimeshiftedSampleSet
        Chris saved the query time to the summary, but took it out here
        """
        with Timer() as query_time:
            data = self.state.head(self.state.endtime, seconds=self.binsize)

        logger.info('picker.run.batch', query_time=query_time)
        return data
    
    def run(self, test=False):
        try:
            endtime = self.state.endtime
            earliesttime = self.state.starttime

            self.__start()
            state = 'done'
            now = datetime.now()
            time.sleep(1) #prev 8
            complete_list=[]

            #RT Test (want to see if what the /conninfo is)
            try:
                row=connect_db(connection)
                logger.info('connect_db.results', meessage=row)
            except:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                traceback.print_exception(exc_type, exc_value, exc_traceback,
                                        limit=2, file=sys.stdout)

                logger.info(
                    'error.connect_db.results',
                    error_message=str(traceback.format_exc())
                )

            with Timer() as self.run_time:
                with Timer() as batch_time:
                    sets = self.batch() 
                    '''
                    Example output set: a list of dictionaries, of all the filtered out sets we want.
                    , {'id': 3388141, 'instrument_name': 'WRC2.CI.--.HH', 'channel_name': 'HHE', 'starttime': 1624435033.96839, 'endtime': 1624435034.95839,
                    '''

                if sets:
                    print("Have sets")
                    with Timer() as get_stations_time:
                        station_list=[]
                        network_list=[]
                        channel_list=[]
                        location_list=[]
                        samprate_list=[]
                        startt_list=[]
                        endt_list=[]
                        datatype_list=[]
                        data_list=[]

                        for i, d in enumerate(sets):
                            try:
                                seismic_station_name=d['instrument_name'].split('.')
                                station=seismic_station_name[0]
                                network=seismic_station_name[1]
                                location=seismic_station_name[2]
                                channel=d['channel_name']
                                samprate=d['samprate']
                                startt=d['starttime']
                                endt=d['endtime']

                                datatype='i4' #default stub
                                data=d['samples']

                                station_list.append(station)
                                network_list.append(network)
                                channel_list.append(channel)
                                location_list.append(location)
                                samprate_list.append(samprate)
                                startt_list.append(startt)
                                endt_list.append(endt)
                                datatype_list.append(datatype)
                                data_list.append(data)
                            except Exception as e: #sometimes we don't get data out of the station, in which case we skip (or continue)
                                print("Error encountered")
                                exc_type, exc_value, exc_traceback = sys.exc_info()
                                traceback.print_exception(exc_type, exc_value, exc_traceback,
                                                        limit=2, file=sys.stdout)

                        data_used = pd.DataFrame(list(zip(station_list,network_list,channel_list, location_list, samprate_list,startt_list,endt_list,datatype_list,data_list)), 
                                    columns =['station','network', 'channel', 'location', 'samprate','startt','endt','datatype','data']) 
                        '''
                        We recovered data of this format:
                        quakes2aws-datastore    | For given 16-second interval
                        quakes2aws-datastore    |       station  ...                                               data
                        quakes2aws-datastore    | 0        WRC2  ...  [1140, 1142, 1134, 1123, 1128, 1129, 1133, 113...
                        quakes2aws-datastore    | 1        WRC2  ...  [-135, -136, -135, -132, -136, -140, -124, -11...
                        quakes2aws-datastore    | 92009     HAR  ...  [-790, -871, -927, -955, -955, -922, -871, -78...
                        quakes2aws-datastore    | 92010     HAR  ...  [928, 897, 842, 766, 668, 560, 449, 331, 221, ...     
                        Sample rate is 100Hz, so 100 samples in the data
                        There can be 92011 rows as seen here
                        '''

                        data_used_filtered=data_used
                        goodinstruments_counter=0
                        totalinstruments_counter=0

                        data_used_filtered['inst'] = data_used_filtered['channel'].astype(str).str[:2]
                        df_candidates = data_used_filtered[['station','network','inst']].drop_duplicates()
                        '''
                        ['WRC2.CI.HH', 'CJV2.CI.HH', 'WNM.CI.EH', 'TJR.CI.EH', 'HDH.CI.HH', 'PDR.CI.HH', 'SRI.CI.HH', 'CKP.CI.HH', 'RHR.CI.HH', 'RSI.CI.HH', 'RAG.CI.HH',  ...']
                        '''
                        number_total_channels=(self.binsize)*3 #3 channels per instrument
                    
                    with Timer() as process_stations_time:
                        complete_list=[]
                        counter=0

                        for _, row in df_candidates.iterrows():
                            with Timer() as filtering_stations_time:
                                totalinstruments_counter+=1
                                df_complete=data_used_filtered[(data_used_filtered['station']==row['station']) & (data_used_filtered['network']==row['network']) & (data_used_filtered['inst']==row['inst'])]
                                df_complete = df_complete.drop_duplicates(subset=['station','network','channel','startt']) #RT!!!! Had to get rid of extra duplicates here too. (but need to keep specific items)

                            if len(df_complete)==number_total_channels-3 or len(df_complete)==number_total_channels-6 or len(df_complete)==number_total_channels or len(df_complete)==number_total_channels+3 or len(df_complete)==number_total_channels+6:
                                goodinstruments_counter+=1
                                data_used_final = df_complete
                                data_used_final_sorted=data_used_final.sort_values(by=['station', 'network','channel','startt'])

                                #This is where we deviate; we don't multiprocess API here, 
                                complete_list.append(data_used_final_sorted) #append dataframes
                                counter+=1
                        
                        number_of_stations=len(complete_list)
                        now = datetime.now() # current date and time
                        time_str = now.strftime("%Y_%m_%d_%H_%M_%S")
                        moddedList=[]

                        for df in complete_list:
                            time_str_dup = [time_str]*len(df['startt'])
                            num_stations = [number_of_stations]*len(df['startt'])
                            df.insert(loc=0, column='timestamp', value=time_str_dup)
                            df.insert(loc=0, column='num_stations', value=num_stations)
                            moddedList.append(df)

                    #chunk the dataset using list comprehension
                    with Timer() as chunk_data:
                        n = 55 
                        final = [moddedList[i * n:(i + 1) * n] for i in range((len(moddedList) + n - 1) // n )] 

                    s3_output_file_one='data_analysis/sample_chunked_' #+str(counter)+'.zip'
                    s3_output_file_two='data_analysis_two/sample_chunked_' #+str(counter)+'.zip'
                    s3_output_list=[s3_output_file_one,s3_output_file_two]

                    counterZip=0
                    with Timer() as aggregate_time:
                        for item in final: #so for each of these 12 items, we have 55 dataframes. Each of these 55 dataframes are 87by12s or 90by12s together in a csv file
                            df1 = pd.DataFrame()
                            df1=df1.append(item)

                            rt_path='sample_chunked.csv' #3.5MBs for 30 seconds of 55 unique station (will be compressed)
                            df1.to_csv(rt_path, index = False, header=True) 

                            counter_used=str(counterZip)+'.zip'
                            desired_zip_file='sample_chunked_'+counter_used
                            # desired_zip_file='sample_chunked_'+str(counterZip)+'.zip'
                            zipObj = ZipFile(desired_zip_file, 'w', zipfile.ZIP_DEFLATED)
                            zipObj.write(rt_path)
                            zipObj.close()

                            counterZip+=1
                            bucket_index = (counterZip) % 2 #alternate between the different buckets in the list
                            s3_output_file_root=s3_output_list[bucket_index]
                            s3_output_file=s3_output_file_root+counter_used
                            s3.upload_to_s3(desired_zip_file, s3_output_file) #will also call the S3 bucket GPD_PickLog with the specific timestamps as well

                    good_instrument_ratio=str(goodinstruments_counter)+'/'+str(totalinstruments_counter)

                    #Below are timing subsets of the entire runtime
                    get_stations_time_elapsed=round(get_stations_time.elapsed,3)
                    process_stations_time_elapsed=round(process_stations_time.elapsed,3)
                    filtering_stations_time_elapsed=round(filtering_stations_time.elapsed,3)
                    chunk_data_elapsed=round(chunk_data.elapsed,3)
                    aggregate_time_elapsed=round(aggregate_time.elapsed,3)

                    logger.info(
                        'runtime.subset.results',
                        good_instrument_ratio=good_instrument_ratio,
                        get_stations_time=get_stations_time_elapsed,
                        process_stations_time=process_stations_time_elapsed,
                        filtering_stations_time=filtering_stations_time_elapsed,
                        subset_to55_time=chunk_data_elapsed,
                        number_zip_files=str(counterZip),
                        zip_fileToS3_time=aggregate_time_elapsed
                    )

                    alltime_elapsed=round(self.run_time.elapsed,3)

                    if (self.counter_trimDB % 1) == 0:
                        #3/28/22 RT added as a means to keep resetting the primary key to avoid hitting limits (will need to see if it doesn't adversely affect sample acquisition)
                        with Timer() as currKey_time:
                            sample_set_curr_key=self.state.check_sampleset_key() #returns the int value of the primary key

                            if sample_set_curr_key>500000000: #if the value exceeds 500 million, which constitutes quite a few runs, we reset to 0 to prevent any hard limit hits 1000000000
                                self.state.reset_sampleset_key()

                        currKey_time_elapsed=round(currKey_time.elapsed,3)

                        logger.info(
                            'runtime.currKeylook.results',
                            currKey_time=currKey_time_elapsed
                        )

                    #Get all global elapsed times to this point (testing)
                    batch_time_elapsed=round(batch_time.elapsed,3)

                    logger.info(
                        'runtime.global.information',
                        latestTS=str(endtime),
                        earliestTS=str(earliesttime),
                        presentTime=self.present_time,
                        currentTS=str(self.starttime)
                    )

                    logger.info(
                        'runtime.global.results',
                        total_process_time=alltime_elapsed,
                        batch_time=batch_time_elapsed,
                        state=state
                    )

                else: #if we exceed the boundaries of the DB, catch up to it
                    state = 'no-data'
                    print("No sets")

            self.__finish(state=state)
            return self.run_time.elapsed
        except Exception as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            traceback.print_exception(exc_type, exc_value, exc_traceback,
                                      limit=2, file=sys.stdout)

            logger.info(
                'error.global.results',
                error_message=str(traceback.format_exc())
            )

            return self.binsize #return a time by default 
    
#Reference PickRunner from here
class PickRunner:
    """
    Run the picker in a loop.
    """
    def __init__(self, starttime=None, count=None, no_sleep=False, live=True, mark_as_processed=True,test=False):
        """
        :param starttime float: start data going forward from this SampleSet starttime
        :param count int: do this many iterations, then stop
        :param no_sleep bool: don't sleep between iterations
        """
        self.state = State(live=live)
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
        self.__stub=self.state.endtime #thought this could be None at first

    def __guess_starttime(self):
        starttime = time.time() - self.PICKER_DELAY_SECONDS - self.binsize
        return starttime

    def __wait_for_enough_data(self):
        """
        Let ``settings.PICKER_DELAY_SECONDS`` seconds accumulate in the staging database before doing picks. This allows
        at least some late arriving packets to get here before we try to pick them.
        """
        while self.state.is_empty:
            logger.info('pick.starttime.no-data', sleep=self.binsize)
            time.sleep(self.binsize)
        wanted_starttime = self.__guess_starttime()
        oldest_sample = self.state.starttime
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
            oldest_sample = self.state.starttime

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

        #Explore trimming the database from here
        counter_trimDB=0 

        while True:
            #One element is to make sure we have caught up to the DB (taken from the job fn)
            start_time_used=self.__starttime
            now = datetime.now() # current date and time
            starting_format = now.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-7]
            present_time=starting_format

            if start_time_used<=self.state.endtime or self.state.endtime != self.__stub:
                logger.info(
                    'beginningOfNewJob',
                    presentTime=present_time,
                    currentTS=str(start_time_used),
                    latestTS=str(self.state.endtime),
                    binSize=self.binsize
                )

                run_time = PickRun(
                    self.state,
                    self.state.endtime,
                    self.binsize,
                    self.samprate,
                    present_time,
                    counter_trimDB,
                    mark_as_processed=self.mark_as_processed
                ).run(test=self.test)

                #Then
                self.__stub=self.state.endtime
                incremented_time=self.starttime #value won't be used, but we NEED THIS to increment

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
                    latestTS=str(self.state.endtime),
                    presentTime=starting_format,
                    binSize=self.binsize,
                    sampRate=self.samprate,
                    currentTS=str(start_time_used)
                )

                #Also trim off idle state, just so we don't keep accumulating data here either
                if (counter_trimDB % 1) == 0:
                    with Timer() as trimDB_time:
                        dt = timezone.now() - timedelta(seconds=300) 
                        horizon = dt.timestamp()

                        #this is what trim does:
                        #SampleSet.objects.filter(starttime__lte=horizon).delete() this is the command we use
                        self.state.trim(horizon)
                    trimDB_time_elapsed=round(trimDB_time.elapsed,3)

                    logger.info(
                        'runtime.trimDB.results',
                        trimDB_time=trimDB_time_elapsed
                    )
            counter_trimDB+=1

if __name__ == '__main__':
    #invoke PickRunner.run() here.

    #We don't have a specified starttime or a specified # of iterations
    #Don't sleep, or timeshift samples to the present, don't mark processed sample sets as processed
    #The while loop exists within the PickRunner
    runner = PickRunner(
        starttime=None,
        count=None,
        no_sleep=False,
        live=not False,
        mark_as_processed=not False
    )
    runner.run(test=False)


    # #Can set a while True, time.sleep later on to simulate, want to make sure it has access to our DB
    # while True:
    #     db_safe_flag=0

    #     #Sanity 
    #     with Timer() as establish_connection:
    #         dt_curr = timezone.now()
    #         dt = dt_curr - timedelta(seconds=300) 
    #         horizon = dt.timestamp()

    #         connection_test=connection.ensure_connection()
    #         # print("connection found")
    #         # print(connection_test)



    #     with Timer() as fetch_time:
    #         try:
    #             row,rowTwo=connect_db(connection)
    #         except:
    #             print("Timeout")
    #             # rowThree='No conn info'
    #             db_safe_flag=1
        
    #     if db_safe_flag==0:
    #         statement="Commence getting waveforms"

    #         #RT 12/8/22 exception here
    #         try:
    #             latest_endtime=row[0][0] #should be of type 'float'
    #         except:
    #             print("Out of range, no latest endtime found")
    #             latest_endtime='N/A'

    #         #RT 12/8/22 exception here
    #         try:
    #             storage_size=rowTwo[0][0] #should be of type 'float'
    #         except:
    #             print("Out of range, no storage size found")
    #             storage_size='N/A'

    #     else:
    #         statement="Ping lambda fn that accesses alternate DB/DynamoDB for data when DB is replenishing storage"
    #         latest_endtime='DB replenish'
    #         storage_size='N/A'


    #     fetch_time_items_elapsed=round(fetch_time.elapsed,3)
    #     establishconn_items_elapsed=round(establish_connection.elapsed,3)

    #     #Also want to print out what we see in the connection test as well
    #     logger.info(
    #         'info',
    #         connect_time=establishconn_items_elapsed,
    #         fetch_time=fetch_time_items_elapsed,
    #         latest_endtime_found=latest_endtime,
    #         statement_used=statement,
    #         storage_size=storage_size,
    #         connectiontest=connection_test)
        
    #     #12/12/22 RT update: We don't want overruns on time here, but can have overruns (if the fetch time exceeds 30 seconds, then we can't sleep for a negative #,
    #     # this would error out)
    #     if fetch_time_items_elapsed<=30:
    #         time_sleeping=30-fetch_time_items_elapsed
    #         time.sleep(time_sleeping) #30-the time it took to fetch
    #     #otherwise, no sleeping, it goes to the next loop immediately


    # # app.run(host='0.0.0.0', port=8080)