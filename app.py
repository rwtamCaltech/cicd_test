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

#We are testing a unit test area:
from flask import Flask
app = Flask(__name__)

#Did a test where after the client is first loaded, it should be much faster to run continuously and ingest data back. 
session = boto3.Session()
query_client = session.client('timestream-query')
query_example = QueryExample(query_client)

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
                    #RT No more self.batch()
                    #DERIVED from components\datastore\quakes2aws_datastore\core\models.py whwere our headfn helps
                    initial_starttime=endtime-self.binsize-6
                    final_starttime=endtime-5

                    #STOPPED here. We will create a functiont that will allow us to query the data. This might be the real pain point, since
                    #this query seems to take a while with local testing (relying on the Fargate instance being in same us-west-2 as Timestream DB
                    # to make it faster, but not sure)

                    query_specified=f"""
                        SELECT * FROM devops_rt.host_metrics_rt WHERE (measure_value::double>"""+str(initial_starttime)+""" AND measure_value::double<"""+str(final_starttime)+""")
                    """
                    all_query_results,gbytes_scanned_thirty,gbytes_metered_thirty,cost_for_query_thirty=query_example.run_rt_query(query_specified)
                    # sets = self.batch() 
                
                #At this point, we see if we have results
                queried_results=len(all_query_results)

                if queried_results!=0:
                    with Timer() as get_stations_time:
                        #similar to reposRevLambV3, start aggregating the data here
                        #components\datastore\quakes2aws_datastore\picker\tasks.py
                        station_list=[]
                        network_list=[]
                        channel_list=[]
                        location_list=[]
                        samprate_list=[]
                        startt_list=[]
                        endt_list=[]
                        data_list=[]

                        for result in all_query_results:
                            '''
                            As an example:
                            {'Startt': '1649776945', 'Station': 'PLM', 'CurrentTimestamp': '2022_04_12_15_27_30', 'ChannelAll': 'HNE', 'ChannelTwoBit': 'HN', 'Network': 'CI', 
                            'Endt': '1649776946', 'SeismicDataOneSecArray': '[14296, 14301, 14298, 14301, 14297, 14297, 14301, 14300, 14300, 14297, 14293, 14298, 14303, 14306, 
                            14296, 14290, 14296, 14303, 14305, 14293, 14298, 14300, 14300, 14300, 14293, 14297, 14305, 14305, 14298, 14294, 14299, 14304, 14303, 14297, 14295, 
                            14300, 14299, 14299, 14299, 14298, 14298, 14304, 14298, 14295, 14301, 14301, 14304, 14298, 14296, 14297, 14299, 14304, 14298, 14294, 14299, 14302, 
                            14301, 14297, 14297, 14299, 14300, 14305, 14296, 14293, 14299, 14304, 14303, 14298, 14297, 14300, 14302, 14298, 14298, 14301, 14299, 14297, 14301, 
                            14298, 14295, 14295, 14301, 14302, 14295, 14299, 14300, 14300, 14303, 14298, 14295, 14301, 14297, 14301, 14299, 14296, 14299, 14294, 14304, 14302, 
                            14292, 14297]', 'Location': '--', 'measure_name': 'SampRate', 'time': '2023-01-06 01:20:38.011000000', 'measure_value::double': '100.0'} 
                            '''
                            array_used=result['SeismicDataOneSecArray']
                            array_datalist = json.loads(array_used) #converts to a list that we can read the numbers off of (these numbers are of type int); otherwise str

                            station_list.append(result['Station'])
                            network_list.append(result['Network'])
                            channel_list.append(result['ChannelAll'])
                            location_list.append(result['Location'])
                            samprate_list.append(result['SampRate'])
                            startt_list.append(int(float(result['measure_value::double']))) #had to convert to FLOAT, not int, since it was a str
                            endt_list.append(int(float(result['Endt']))) #had to convert to float since it was a str
                            data_list.append(array_datalist) #a list of lists
                            #Perhaps add an error exception once we get to more rigorous testing

                        data_used = pd.DataFrame(list(zip(station_list,network_list,channel_list, location_list, samprate_list,startt_list,endt_list,data_list)), 
                                    columns =['station','network', 'channel', 'location', 'samprate','startt','endt','data']) 
                        '''
                        As an example below:
                        station network channel  ...      startt        endt                                               data
                        0     PLM      CI     HNE  ...  1649776945  1649776946  [14296, 14301, 14298, 14301, 14297, 14297, 143...
                        1    JNH2      CI     HNE  ...  1649776945  1649776946  [-18436, -18437, -18439, -18437, -18433, -1843...
                        2     WHF      CI     HHE  ...  1649776945  1649776946  [458, 473, 489, 481, 484, 490, 482, 461, 458, ...
                        '''
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
                        totalinstruments_counter=0
                        goodinstruments_counter=0

                    with Timer() as process_stations_time:
                        complete_list=[]
                        counter=0
                        channel_breakdown_station_list=[]

                        #RT 2/13/23 ADD: 
                        all_stationsets_acquired=[]

                        for _, row in df_candidates.iterrows():
                            with Timer() as filtering_stations_time:
                                totalinstruments_counter+=1
                                df_complete=data_used[(data_used['station']==row['station']) & (data_used['network']==row['network']) & (data_used['inst']==row['inst'])]
                                df_complete = df_complete.drop_duplicates(subset=['station','network','channel','startt']) #RT!!!! Had to get rid of extra duplicates here too. (but need to keep specific items)

                            #if we have the right number of channels, we continue to process
                            #we might get EH channels, those aren't enough to process; HH and HN are likely most common
                            #7/2/21 RT note: sometimes we can get 17 seconds of data bc we did a +1; starttime + seconds + 1
                            if len(df_complete)==number_total_channels-3 or len(df_complete)==number_total_channels-6 or len(df_complete)==number_total_channels or len(df_complete)==number_total_channels+3 or len(df_complete)==number_total_channels+6:
                                goodinstruments_counter+=1
                                data_used_final = df_complete
                                data_used_final_sorted=data_used_final.sort_values(by=['station', 'network','channel','startt'])
                                complete_list.append(data_used_final_sorted) #append dataframes
                                channel_breakdown_station_list.append(row['inst'])

                                #RT 2/13/23 ADD: 
                                all_stationsets_acquired.append(row['network']+'.'+row['station']+'.'+row['inst'])

                                counter+=1
                        
                        #RT 2/13/23 ADD; as part of the process_stations, we will also make comparisons with all the stations we get at that 30-second increment:
                        #GUIDANCE FROM: ryanTests\stationMonitoring.py
                        all_stationsets_acquired.sort()

                        #STEP 1: COMPARE TO ELLEN'S GLOBAL/LATEST STATION DATA FIRST. 
                        text_file='active_RT_primary_chan.txt'
                        df = pd.read_fwf(text_file, colspecs='infer')
                        df["Period"] = df['NET']+"."+ df["STA"]+"."+ df["SEE"].astype(str).str[:2] #This gets the aggregations we desire
                        station_match_with_text=[]
                        station_nomatch_with_text=[]

                        for station_set in all_stationsets_acquired:
                            if station_set in df["Period"].tolist():
                                station_match_with_text.append(station_set)
                            else:
                                station_nomatch_with_text.append(station_set)

                        #STEP 2: COMPARE OUR ANTARCTIC Q330s, and see what stations we get back here (MORE DIRECT TO THE DATA WE ARE FUNNELING OUT)
                        #STEP 3: Also will get the nonq330s, but code is written a similar way so in for loop
                        q330_antarctic='antarctic_q330.txt'
                        nonq330_antarctic='antarctic_nonq330.txt'

                        station_list=[q330_antarctic,nonq330_antarctic]
                        countmatchq_stations=[]
                        countnomatchq_stations=[]
                        matchq_stations=[]
                        nomatchq_stations=[]

                        for station_items in station_list:
                            # print("For stations: "+station_items)
                            df_station = pd.read_fwf(station_items, colspecs='infer',header=None)
                            df_station = df_station.drop(2, axis=1).join(df_station[2].str.split(expand=True), rsuffix='_x')

                            df_channel_sep = pd.DataFrame([ x.split(',') for x in df_station[4].tolist() ])
                            num_columns=len(df_channel_sep.columns)

                            newDF = pd.DataFrame()

                            for i in range(num_columns):
                                df_series = df_station["1"]+"."+ df_station['0_x']+"."+df_channel_sep[i].astype(str).str[:2]
                                df_used=df_series.to_frame()
                                newDF=newDF.append(df_used,ignore_index = True)

                            allq330_stations=newDF.dropna()
                            station_match_with_text=[]
                            station_nomatch_with_text=[]

                            for station_set in all_stationsets_acquired:
                                if station_set in allq330_stations[0].tolist():
                                    station_match_with_text.append(station_set)
                                else:
                                    station_nomatch_with_text.append(station_set)

                            #Now we can see which stations match what we have in the list.
                            countmatchq_stations.append(len(station_match_with_text))
                            countnomatchq_stations.append(len(station_nomatch_with_text))
                            matchq_stations.append(station_match_with_text)
                            nomatchq_stations.append(station_nomatch_with_text)


                        #STEP 4: LIST OF WEIQIANG'S STATIONS THAT HE USED FOR RIDGECREST ANALYSIS (so we can see if we can have an apples-to-apples comparison based on the stations he's using)
                        #Do from Weiqiang's perspective; see what stations from his file that we are missing, at any given time. 
                        Weiqiang_Ridgecrest_stations='stations_Weiqiang_Ridgecrest.csv'
                        df_station=pd.read_csv(Weiqiang_Ridgecrest_stations,sep='\t') #52 columns; Weiqiang relied on 52 stations to make his predictions and get results back. 
                        df_station[['net','stat','extra','chan']] = df_station.station.str.split(".",expand=True)
                        df_station['tot'] = df_station["net"]+"."+ df_station['stat']+"."+df_station['chan'] #51 stations, now omitting the ones with 2C in front of the channels

                        weiqiang_station_found_list=[]
                        weiqiang_station_notfound_list=[]
                        for weiqiang_stations in df_station['tot'].tolist():
                            if weiqiang_stations in all_stationsets_acquired:
                                weiqiang_station_found_list.append(weiqiang_stations)
                            else:
                                weiqiang_station_notfound_list.append(weiqiang_stations)

                        #LOOKS LIKE ALL THE DATA IS SORTED, so no need: all_stationsets_acquired.sort()
                        logger.info(
                            'station.match.results',
                            ourstation_foundellen_global_list=str(station_match_with_text),
                            ourstation_notfoundellen_global_list=str(station_nomatch_with_text),
                            ourstation_foundq330_global_list=str(matchq_stations[0]),
                            ourstation_notfoundq330_global_list=str(nomatchq_stations[0]),
                            ourstation_foundNonq330_global_list=str(matchq_stations[1]),
                            ourstation_notfoundNonq330_global_list=str(nomatchq_stations[1]),
                            weiqiangRidgecrest_found_global_list=str(weiqiang_station_found_list),
                            weiqiangRidgecrest_notfound_global_list=str(weiqiang_station_notfound_list)
                        )


                        logger.info(
                            'station.match.count.results',
                            ourstation_foundellen_global_list=str(len(station_match_with_text)),
                            ourstation_notfoundellen_global_list=str(len(station_nomatch_with_text)),
                            ourstation_foundq330_global_list=str(countmatchq_stations[0]),
                            ourstation_notfoundq330_global_list=str(countnomatchq_stations[0]),
                            ourstation_foundNonq330_global_list=str(countmatchq_stations[1]),
                            ourstation_notfoundNonq330_global_list=str(countnomatchq_stations[1]),
                            weiqiangRidgecrest_found_global_list=str(len(weiqiang_station_found_list)),
                            weiqiangRidgecrest_notfound_global_list=str(len(weiqiang_station_notfound_list))
                        )

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


                    with Timer() as get_unique_channels_time:
                        unique_channels_found=list(set(channel_breakdown_station_list))
                        unique_station_str=''
                        count_unique_station_str=''
                        for index, unique_channel in enumerate(unique_channels_found):
                            spec_channel_count=channel_breakdown_station_list.count(unique_channel)

                            if index!=len(unique_channels_found)-1: #not at the end of the list
                                unique_channel_mod=unique_channel+','
                                spec_channel_mod=str(spec_channel_count)+','
                            else: #end of the list
                                unique_channel_mod=unique_channel
                                spec_channel_mod=str(spec_channel_count)
                            
                            unique_station_str+=unique_channel_mod
                            count_unique_station_str+=spec_channel_mod

                    good_instrument_ratio=str(goodinstruments_counter)+'/'+str(totalinstruments_counter)

                    #Below are timing subsets of the entire runtime
                    get_stations_time_elapsed=round(get_stations_time.elapsed,3)
                    process_stations_time_elapsed=round(process_stations_time.elapsed,3)
                    filtering_stations_time_elapsed=round(filtering_stations_time.elapsed,3)
                    chunk_data_elapsed=round(chunk_data.elapsed,3)
                    aggregate_time_elapsed=round(aggregate_time.elapsed,3)
                    get_unique_channels_time_elapsed=round(get_unique_channels_time.elapsed,3)


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

                    # logger.info(
                    #     'runtime.global.results',
                    #     total_process_time=alltime_elapsed,
                    #     batch_time=batch_time_elapsed,
                    #     state=state
                    # )
                    #1/17/23 RT update: Want to add the querying information: gbytes_scanned_thirty,gbytes_metered_thirty,cost_for_query_thirty
                    logger.info(
                        'runtime.global.results',
                        total_process_time=alltime_elapsed,
                        batch_time=batch_time_elapsed,
                        batch_gbytes_scanned=gbytes_scanned_thirty,
                        batch_gbytes_metered=gbytes_metered_thirty,
                        batch_query_cost=cost_for_query_thirty,
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

        #Ryan (we won't need State)
        # self.state = State(live=live)
        #We will call a function to get the maximum starttime at the moment
        max_starttime, record_maxtime_seismicquery_time_elapsed,gbytes_scanned_tot,gbytes_metered_tot,cost_for_query=self.__find_maxstarttime()

        logger.info('max_timestream.query', query_time_elapsed=record_maxtime_seismicquery_time_elapsed,max_starttime=max_starttime,gbytes_scanned=gbytes_scanned_tot,gbytes_metered=gbytes_metered_tot,query_cost=cost_for_query)
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
        self.__stub=max_starttime #thought this could be None at first


    def __find_maxstarttime(self):
        # query_specified_maxtime=f"""
        #     SELECT MAX(measure_value::double) FROM devops_rt.host_metrics_rt
        # """

        #Get the latest data, so we can reduce the query space for this 
        query_specified_maxtime=f"""
            SELECT MAX(measure_value::double) FROM devops_rt.host_metrics_rt WHERE time between ago(1m) and now() 
        """

        with Timer() as record_maxtime_seismicquery_time:
            all_query_results,gbytes_scanned_tot,gbytes_metered_tot,cost_for_query=query_example.run_rt_query(query_specified_maxtime)

            for result in all_query_results:
                try:
                    self.max_starttime=int(float(result['_col0']))
                except: #I am presuming that if the DB is cleared after several days, there is nothing in the DB, so say 'None'
                    self.max_starttime=None
                break

        record_maxtime_seismicquery_time_elapsed=round(record_maxtime_seismicquery_time.elapsed,3)
        return self.max_starttime, record_maxtime_seismicquery_time_elapsed,gbytes_scanned_tot,gbytes_metered_tot,cost_for_query #We will update the max_starttime each time we run this, so we ca reference it


    def __find_minstarttime(self):
        # query_specified_maxtime=f"""
        #     SELECT MIN(measure_value::double) FROM devops_rt.host_metrics_rt
        # """

        query_specified_maxtime=f"""
            SELECT MIN(measure_value::double) FROM devops_rt.host_metrics_rt WHERE time between ago(1m) and now()
        """

        with Timer() as record_mintime_seismicquery_time:
            all_query_results,gbytes_scanned_tot,gbytes_metered_tot,cost_for_query=query_example.run_rt_query(query_specified_maxtime)

            for result in all_query_results:
                try:
                    self.min_starttime=int(float(result['_col0']))
                except: #I am presuming that if the DB is cleared after several days, there is nothing in the DB, so say 'None'
                    self.min_starttime=None
                break

        record_mintime_seismicquery_time_elapsed=round(record_mintime_seismicquery_time.elapsed,3)
        return self.min_starttime, record_mintime_seismicquery_time_elapsed,gbytes_scanned_tot,gbytes_metered_tot,cost_for_query #We will update the max_starttime each time we run this, so we ca reference it

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
        min_starttime, _, _, _, _=self.__find_minstarttime()
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
            min_starttime, _, _, _, _=self.__find_minstarttime()
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

            max_starttime, record_maxtime_seismicquery_time_elapsed,gbytes_scanned_tot_max,gbytes_metered_tot_max,cost_for_query_max=self.__find_maxstarttime()
            # min_starttime, record_mintime_seismicquery_time_elapsed,gbytes_scanned_tot_min,gbytes_metered_tot_min,cost_for_query_min=self.__find_minstarttime()
            tot_query_elapsed=record_maxtime_seismicquery_time_elapsed
            gbytes_scanned_all=gbytes_scanned_tot_max
            gbytes_metered_all=gbytes_metered_tot_max
            cost_for_query_all=cost_for_query_max

            #Recall that start_time_used is starttime = time.time() - self.PICKER_DELAY_SECONDS - self.binsize, so based off the current time we are running.
            #However, we only increment this start time if we get to the beginningOfNewJob, so we need to store new data continuously.
            logger.info('monitor_initial', currentTS=start_time_used, latestTS=max_starttime,stubValue=self.__stub, query_time_elapsed=tot_query_elapsed,gbytes_scanned=gbytes_scanned_all,gbytes_metered=gbytes_metered_all,cost_query=cost_for_query_all)

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
