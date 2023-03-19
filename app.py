"""
Main application file
Works with additional logging statements, test Dependabot
added secret keys
"""
import logging, time, os, sys, traceback
import pandas as pd
# import psutil  #already installed as part of python
import django

#briefly commented
#Testing
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'quakes2aws_datastore.settings')
django.setup()

# from django.utils import timezone
from quakes2aws_datastore.logging import logger  # noqa:E402

# from quakes2aws_datastore.core.models import State #We won't be using the PostgresDB so State is not important
import quakes2aws_datastore.s3 as s3
from contexttimer import Timer
from QueryExample import QueryExample
from datetime import datetime
from zipfile import ZipFile
import zipfile
import boto3
import json
import ast

#multiprocessing avenues
from multiprocessing import Process, Queue
from math import ceil
import multiprocessing 

#REDIS used here
from rediscluster import RedisCluster


#We are testing a unit test area:
from flask import Flask
app = Flask(__name__)

#Did a test where after the client is first loaded, it should be much faster to run continuously and ingest data back. 
#3.16.23 UPDATE:
redis = RedisCluster(startup_nodes=[{"host": "redis-cluster.1ge2d2.clustercfg.usw2.cache.amazonaws.com","port": "6379"}], decode_responses=True,skip_full_coverage_check=True)
query_example = QueryExample(redis)
# session = boto3.Session()
# query_client = session.client('timestream-query')

# Initialize Logger
LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)


@app.route('/<random_string>')
def returnBackwardsString(random_string):
    """Reverse and return the provided URI"""
    LOGGER.info('Received a message: %s', random_string)
    return "".join(reversed(random_string))


class PickRun:
    """
    Run a single pick iteration.

    Ryan notes; took out fn: def mark_sets_as_processed(self, set_ids); don't think we use them.
    """
    # def __init__(self, starttime, min_starttime,binsize, samprate, present_time,mark_as_processed=True):
    def __init__(self, starttime,binsize, samprate, present_time,mark_as_processed=True):
        # self.state = state
        self.starttime = starttime
        # self.min_starttime=min_starttime
        self.binsize = binsize
        self.samprate = samprate
        self.present_time=present_time
        self.mark_as_processed = mark_as_processed

    #Took out a lot of DB type saves in the start and finish sections here. Don't save pick_run_stats or summary at start and finish, respectively.
    def __start(self):
        logger.info('picker.run.start', starttime=self.starttime, binsize=self.binsize)

    def __finish(self, state='done'):
        logger.info('picker.run.finish')


    def issue_request(self,subset,data_used,number_total_channels,complete_list,channel_breakdown_station_list,missing_waveform_stations,result_queue):
        totalinstruments_counter=0
        goodinstruments_counter=0

        for combo in subset:
            combo_split=combo.split('.')
            network=combo_split[0]
            station=combo_split[1]
            inst=combo_split[2]

            totalinstruments_counter+=1
            df_complete=data_used[(data_used['station']==station) & (data_used['network']==network) & (data_used['inst']==inst)]
            df_complete = df_complete.drop_duplicates(subset=['station','network','channel','startt']) #RT!!!! Had to get rid of extra duplicates here too. (but need to keep specific items)

            if len(df_complete)==number_total_channels-3 or len(df_complete)==number_total_channels-6 or len(df_complete)==number_total_channels or len(df_complete)==number_total_channels+3 or len(df_complete)==number_total_channels+6:
                goodinstruments_counter+=1
                data_used_final = df_complete
                data_used_final_sorted=data_used_final.sort_values(by=['station', 'network','channel','startt'])
                complete_list.append(data_used_final_sorted) #append dataframes
                channel_breakdown_station_list.append(inst)
                # counter+=1
            else: #RT added 2/23/23 to see if we are missing any specific waveforms consistently in our runs
                missing_waveform_stations.append(combo)

        
        #complete_list (and maybe channel_breakdown_station_list, for later; are the only things we want to return)
        result_queue.put([complete_list,channel_breakdown_station_list,goodinstruments_counter,totalinstruments_counter,missing_waveform_stations])

    
    def run(self, test=False):
        try:
            #We pass in the starttime and endtime values to PickRun to get the latest times there
            endtime = self.starttime
            # earliesttime = self.min_starttime #RT 1/17/23 DON'T NEED THIS, just reduce number of queries to save hits
            # endtime = self.state.endtime
            # earliesttime = self.state.starttime

            self.__start()
            state = 'done'
            now = datetime.now()
            time.sleep(1) #prev 8
            complete_list=[]

            with Timer() as self.run_time:
                with Timer() as batch_time:
                    #3/17/23 RT: First thing I want to do is a memory check
                    with Timer() as memoryAtBegin_time:
                        string_memAtBegin=query_example.get_memory_usage()

                    #3/17/23 RT UPDATE: (now we can run the querying of our data and see what we get here)
                    #DERIVED from components\datastore\quakes2aws_datastore\core\models.py whwere our headfn helps; #RT No more self.batch()
                    initial_starttime=endtime-self.binsize-5 #-6 at first, but let's try -5 here. 
                    final_starttime=endtime-5

                    all_query_results=query_example.run_rt_query(initial_starttime,final_starttime)

                #At this point, we see if we have results
                queried_results=len(all_query_results)

                if queried_results!=0:
                    with Timer() as get_stations_time:
                        #RT 3/17/23 I'm going to try a new technique instead of iterating across each query result.
                        list_dicts=[ast.literal_eval(eq_query) for eq_query in all_query_results]
                        data_used=pd.DataFrame(list_dicts) #very fast to cobble up a dataframe with all of our desired data

                        #RT transform the 'data' to be list of lists of data, which is what we need 
                        data_list = [json.loads(data_item) for data_item in data_used['data'].tolist()] #to convert the list of strings to a list of lists
                        data_used['data']=data_list #replacing the data column values with the list values in this list

                        data_used['inst'] = data_used['channel'].astype(str).str[:2]
                        df_candidates = data_used[['station','network','inst']].drop_duplicates()

                        '''
                        Candidates found
                            station network inst
                        0       JNH2      CI   HH
                        1        SNO      CI   HN
                        3        LRL      CI   HN
                        5        WMF      CI   HH
                        7       LRR2      CI   HH
                        ^^Example unique stations that were pulled back from 
                        '''
                        #Very important (we are processing the last 30 seconds of data, and we get three channels for each station). So for each unique station combination
                        #we pick out, we expect 90 rows or thereabouts
                        number_total_channels=self.binsize*3
                        totalinstruments_counter_ov=0
                        goodinstruments_counter_ov=0

                        #2/22/23 RT update: This is the start of the multiprocessing data we have
                        #The goal is to compartmentalize all candidate stations to a list, and work off these instead, potentially:
                        df_candidates['tot'] = df_candidates["network"]+"."+ df_candidates['station']+"."+df_candidates['inst']
                        df_candidates_list=df_candidates['tot'].tolist()

                        numOfCores = multiprocessing.cpu_count()

                        if len(df_candidates_list) <= multiprocessing.cpu_count():
                            # print("The number of events (", len(df_candidates_list),") < number of cores (", str(numOfCores),"), using all available cores (",str(numOfCores),") instead")
                            numOfCores = len(df_candidates_list)

                        num_processes_str="Dividing requests between", str(numOfCores), "process(es)"
                        step  = ceil(len(df_candidates_list)/numOfCores) #if 8 processes, 4 steps for 26 events (so each core would take 4 steps)
                        logger.info(
                            'process.stations.div',
                            num_cpu_cores_process=num_processes_str
                        )

                    with Timer() as process_stations_time:
                        complete_list=[]
                        counter=0
                        channel_breakdown_station_list=[]
                        missing_waveform_stations=[]

                        #2/22/23 RT update: Multiprocessing thread, from ryanTests QueryRealTimeExModMProc.py; local testing revealed this cut processing time by 2.2x
                        #let's see if that is what happens here
                        start_index = 0
                        end_index = step
                        predict_workers=[]
                        result_queue = Queue() 

                        for i in range(0, numOfCores):
                            subset=df_candidates_list[start_index:end_index]

                            if not subset:
                                break
                            else:
                                predict_worker=Process(target=self.issue_request, args=(subset,data_used,number_total_channels,complete_list,channel_breakdown_station_list,missing_waveform_stations,result_queue)) 
                                predict_workers.append(predict_worker)
                                predict_worker.start()

                            start_index += step
                            end_index += step

                        results = [result_queue.get() for w in predict_workers]

                        for w in predict_workers:
                            w.join()

                        complete_list_modded=[]
                        channelbrk_list_modded=[]
                        missing_waveform_totlist=[]
                        for result in results:
                            prediction_items=result[0]
                            inst_items=result[1]
                            goodinstruments_counter=result[2]
                            totalinstruments_counter=result[3]
                            missing_waveform_stations_items=result[4]
                            complete_list_modded.extend(prediction_items)
                            channelbrk_list_modded.extend(inst_items)
                            goodinstruments_counter_ov+=goodinstruments_counter
                            totalinstruments_counter_ov+=totalinstruments_counter
                            missing_waveform_totlist.extend(missing_waveform_stations_items)
                                    
                        number_of_stations=len(complete_list_modded)

                        now = datetime.now() # current date and time
                        time_str = now.strftime("%Y_%m_%d_%H_%M_%S")
                        moddedList=[]

                        for df in complete_list_modded:
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


                    #To get the unique stations, had to change to: channelbrk_list_modded
                    with Timer() as get_unique_channels_time:
                        unique_channels_found=list(set(channelbrk_list_modded))
                        unique_station_str=''
                        count_unique_station_str=''
                        for index, unique_channel in enumerate(unique_channels_found):
                            spec_channel_count=channelbrk_list_modded.count(unique_channel)

                            if index!=len(unique_channels_found)-1: #not at the end of the list
                                unique_channel_mod=unique_channel+','
                                spec_channel_mod=str(spec_channel_count)+','
                            else: #end of the list
                                unique_channel_mod=unique_channel
                                spec_channel_mod=str(spec_channel_count)
                            
                            unique_station_str+=unique_channel_mod
                            count_unique_station_str+=spec_channel_mod

                    good_instrument_ratio=str(goodinstruments_counter_ov)+'/'+str(totalinstruments_counter_ov)

                    #Below are timing subsets of the entire runtime
                    get_stations_time_elapsed=round(get_stations_time.elapsed,3)
                    process_stations_time_elapsed=round(process_stations_time.elapsed,3)
                    # filtering_stations_time_elapsed=round(filtering_stations_time.elapsed,3)
                    chunk_data_elapsed=round(chunk_data.elapsed,3)
                    aggregate_time_elapsed=round(aggregate_time.elapsed,3)
                    get_unique_channels_time_elapsed=round(get_unique_channels_time.elapsed,3)


                    #2/23/23 RT: These print out the list of stations that do NOT have the three channels (and thus don't have picks).
                    #If we consistently get stations in this list over many 30-second increments, then that station isn't fully emitting to its potential, and might
                    #need to be debugged/flagged. Not my problem - likely a hardware side aspect. 
                    logger.info(
                        'waveform.nothreechannel.stations',
                        waveform_nothreechannel_list=str(missing_waveform_totlist)
                    )

                    #filtering_stations_time=filtering_stations_time_elapsed, #DON'T NEED THIS (very fast anyway, will find out overhead with process_stations_time)
                    logger.info(
                        'runtime.subset.results',
                        good_instrument_ratio=good_instrument_ratio,
                        get_stations_time=get_stations_time_elapsed,
                        process_stations_time=process_stations_time_elapsed,
                        subset_to55_time=chunk_data_elapsed,
                        number_zip_files=str(counterZip),
                        zip_fileToS3_time=aggregate_time_elapsed
                    )

                    alltime_elapsed=round(self.run_time.elapsed,3)

                    #Get all global elapsed times to this point (testing)
                    batch_time_elapsed=round(batch_time.elapsed,3)

                    # logger.info(
                    #     'runtime.global.information',
                    #     latestTS=str(endtime),
                    #     earliestTS=str(earliesttime),
                    #     presentTime=self.present_time,
                    #     currentTS=str(self.starttime)
                    # )

                    logger.info(
                        'runtime.global.information',
                        latestTS=str(endtime),
                        presentTime=self.present_time,
                        currentTS=str(self.starttime),
                        uniqueStations_process_time=get_unique_channels_time_elapsed,
                        channelsFound=unique_station_str,
                        channelsNumberOfEach=count_unique_station_str
                    )


                    #3/17/23 at the end of all this, we expire all data that we are ingesting that is at least 60 seconds old, to keep our nodes fresh
                    with Timer() as expire_time:
                        all_query_results=query_example.expire()

                    #after expiration, we do a memory check for each node
                    with Timer() as memoryAfterExpir_time:
                        string_memAfterExpir=query_example.get_memory_usage()

                    expire_time_elapsed=round(expire_time.elapsed,3)
                    memoryAtBegin_time_elapsed=round(memoryAtBegin_time.elapsed,3)
                    memoryAfterExpir_time_elapsed=round(memoryAfterExpir_time.elapsed,3)

                    #to compare if we are cleaning up the memory
                    logger.info(
                        'memorycheck.results',
                        memory_atBegin=string_memAtBegin,
                        memory_afterExpir=string_memAfterExpir
                    )

                    #1/17/23 RT update: Want to add the querying information: gbytes_scanned_thirty,gbytes_metered_thirty,cost_for_query_thirty
                    logger.info(
                        'runtime.globalquery.results',
                        total_process_time=alltime_elapsed,
                        batch_time=batch_time_elapsed,
                        expire_time=expire_time_elapsed,
                        access_memoryAtBeginning_time=memoryAtBegin_time_elapsed,
                        access_memoryAfterExpire_time=memoryAfterExpir_time_elapsed,
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
            maxtime_found=query_example.get_max_timestamp_query()

            if not maxtime_found: #if the query was empty, then we 
                self.max_starttime=None #Set the maximum value to none here if there is nothing in the DB
            else: 
                self.max_starttime=int(maxtime_found) #maxtime_found is of type float, we cast it to int here

        record_maxtime_seismicquery_time_elapsed=round(record_maxtime_seismicquery_time.elapsed,3)
        return self.max_starttime, record_maxtime_seismicquery_time_elapsed #We will update the max_starttime each time we run this, so we ca reference it

    #3/15/23 find minimum time. 
    def __find_minstarttime(self):
        with Timer() as record_mintime_seismicquery_time:
            mintime_found=query_example.get_min_timestamp_query()

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

        while True:
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
            if start_time_used<=max_starttime or max_starttime != self.__stub:
                logger.info(
                    'beginningOfNewJob',
                    presentTime=present_time,
                    currentTS=str(start_time_used),
                    latestTS=str(max_starttime),
                    binSize=self.binsize
                )

                run_time = PickRun(
                    self.max_starttime,
                    self.binsize,
                    self.samprate,
                    present_time,
                    mark_as_processed=self.mark_as_processed
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
