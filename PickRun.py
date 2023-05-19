from quakes2aws_datastore.logging import logger  # noqa:E402
from datetime import datetime
import time, sys, traceback
from contexttimer import Timer
import ast
import pandas as pd

#multiprocessing avenues
from multiprocessing import Process, Queue
from math import ceil
import multiprocessing 

from zipfile import ZipFile
import zipfile

import quakes2aws_datastore.s3 as s3
import json

class PickRun:
    """
    Run a single pick iteration.

    Ryan notes; took out fn: def mark_sets_as_processed(self, set_ids); don't think we use them.
    """
    # def __init__(self, starttime, min_starttime,binsize, samprate, present_time,mark_as_processed=True):
    def __init__(self, starttime,binsize, samprate, present_time,mark_as_processed=True,querymech='Base',querypick='Base'):
        # self.state = state
        self.starttime = starttime
        # self.min_starttime=min_starttime
        self.binsize = binsize
        self.samprate = samprate
        self.present_time=present_time
        self.mark_as_processed = mark_as_processed

        self.querymechPickRun=querymech
        self.querypickPickRun=querypick

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

            if len(df_complete)==number_total_channels-15 or len(df_complete)==number_total_channels-12 or len(df_complete)==number_total_channels-9 or len(df_complete)==number_total_channels-3 or len(df_complete)==number_total_channels-6 or len(df_complete)==number_total_channels or len(df_complete)==number_total_channels+3 or len(df_complete)==number_total_channels+6 or len(df_complete)==number_total_channels+9 or len(df_complete)==number_total_channels+12:
            # if len(df_complete)==number_total_channels-3 or len(df_complete)==number_total_channels-6 or len(df_complete)==number_total_channels or len(df_complete)==number_total_channels+3 or len(df_complete)==number_total_channels+6:
                goodinstruments_counter+=1
                data_used_final = df_complete
                data_used_final_sorted=data_used_final.sort_values(by=['station', 'network','channel','startt'])
                complete_list.append(data_used_final_sorted) #append dataframes
                channel_breakdown_station_list.append(inst)
                # counter+=1
            else: #RT added 2/23/23 to see if we are missing any specific waveforms consistently in our runs

                #4/7/23 WITH REDIS, there are occasions when for some reason, we are not getting the full amount of stations. I want to drill down on this and print 
                #the len(df_complete) where len(df_complete) exceeds say 60, something close to what we would expect. Then we can see if we "close" to getting the data at 
                #that point. 
                if len(df_complete)>60:

                    #STEP 1:
                    #I want to see all the actual missing channels I am getting here, and see breakdown of their amounts
                    missing_channel_list=df_complete['channel'].tolist() #ie HHZ,HHN, HHE, and whatever other channels might possibly exist as part of that data?
                    unique_misschannels_found=list(set(missing_channel_list))
                    unique_missstation_str=''
                    count_missunique_station_str=''
                    for index, unique_channel in enumerate(unique_misschannels_found):
                        spec_channel_count=missing_channel_list.count(unique_channel)

                        if index!=len(unique_misschannels_found)-1: #not at the end of the list
                            unique_channel_mod=unique_channel+','
                            spec_channel_mod=str(spec_channel_count)+','
                        else: #end of the list
                            unique_channel_mod=unique_channel
                            spec_channel_mod=str(spec_channel_count)
                        
                        unique_missstation_str+=unique_channel_mod
                        count_missunique_station_str+=spec_channel_mod


                    #STEP 2: Get all startt's and see what they are
                    # startt_list=df_complete['startt'].tolist() 
                    #I want to make sure all the startt's are within the query intervals, but also that I
                    #am not getting extraneous startt's (maybe coming from other channels)
                    '''
                    IE Within our printed log intervals:
                    quakes2aws::datastore: query.intervals end_interval=1680930872 start_interval=1680930842
                    '''

                    #Below, combo for example is the network.station.inst; IE: CI.BAK.HN;
                    #IE: 'CI.BAK.HN: 83', 'CI.BBS.HH: 88',
                    #channelsFound=unique_station_str,channelsNumberOfEach=count_unique_station_str

                    station_list=combo+': '+str(len(df_complete))

                    #STEP 3: Get the missing station compositions, plus any time aspects to get a comprehensive picture
                    # station_list=combo+': '+str(len(df_complete))+', list start times: '+str(startt_list)+', Missing channel types: '+str(unique_missstation_str)+', Count, missing channel types: '+str(count_missunique_station_str)

                    missing_waveform_stations.append(station_list)

                # missing_waveform_stations.append(combo)

        
        #complete_list (and maybe channel_breakdown_station_list, for later; are the only things we want to return)
        result_queue.put([complete_list,channel_breakdown_station_list,goodinstruments_counter,totalinstruments_counter,missing_waveform_stations])


    def find_candidates(self,all_query_results):
        list_dicts=[ast.literal_eval(eq_query) for eq_query in all_query_results]
        data_used=pd.DataFrame(list_dicts) #very fast to cobble up a dataframe with all of our desired data

        '''
        RT 4/10/23 update: see if this works to get rid of edge cases where we REALLY get other channel bits
        The only channel bits we would care about are E, N, and Z
        https://sparkbyexamples.com/pandas/pandas-delete-rows-based-on-column-value/
        '''
        data_used['channelbit'] = data_used['channel'].astype(str).str[-1]
        # print(data_used['channelbit'].tolist())
        # df2 = df[ (df['Fee'] >= 22000) & (df['Discount'] == 2300)]
        #To really set the restriction for our acceptable data to be only E, N and Z bits 
        data_used = data_used[(data_used['channelbit'] == 'E') | (data_used['channelbit'] == 'N') | (data_used['channelbit'] == 'Z')]

        data_used['inst'] = data_used['channel'].astype(str).str[:2]
        df_candidates = data_used[['station','network','inst']].drop_duplicates()

        #2/22/23 RT update: This is the start of the multiprocessing data we have
        #The goal is to compartmentalize all candidate stations to a list, and work off these instead, potentially:
        df_candidates['tot'] = df_candidates["network"]+"."+ df_candidates['station']+"."+df_candidates['inst']
        df_candidates_list=df_candidates['tot'].tolist()
        return df_candidates_list,data_used

    def run(self, test=False):
        try:
            endtime = self.starttime
            self.__start()
            state = 'done'
            now = datetime.now()
            time.sleep(1) #prev 8
            complete_list=[]

            with Timer() as self.run_time:
                with Timer() as batch_time:
                    #3/17/23 RT: First thing I want to do is a memory check
                    with Timer() as memoryAtBegin_time:
                        string_memAtBegin=self.querymechPickRun.get_memory_usage()

                    #3/17/23 RT UPDATE: (now we can run the querying of our data and see what we get here)
                    #DERIVED from components\datastore\quakes2aws_datastore\core\models.py whwere our headfn helps; #RT No more self.batch()
                    initial_starttime=endtime-self.binsize-5 #-6 at first, but let's try -5 here. 
                    final_starttime=endtime-5

                    #I trust what I am querying, but want to make sure intervals are fine
                    logger.info(
                        'query.intervals',
                        start_interval=str(initial_starttime),
                        end_interval=str(final_starttime)
                    )


                    all_query_results=self.querymechPickRun.run_rt_query(initial_starttime,final_starttime)

                #At this point, we see if we have results
                queried_results=len(all_query_results)

                if queried_results!=0:
                    with Timer() as get_stations_time:
                        #Very important (we are processing the last 30 seconds of data, and we get three channels for each station). So for each unique station combination
                        #we pick out, we expect 90 rows or thereabouts
                        number_total_channels=self.binsize*3
                        totalinstruments_counter_ov=0
                        goodinstruments_counter_ov=0

                        #RT 3/17/23 Wrap it into a function that we can unit test:
                        df_candidates_list,data_used=self.find_candidates(all_query_results)

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

                    #RT 5/8/23 added:
                    # s3_gamma_trigger='triggerGaMMa/sample_chunked_'
                    # s3_gamma_trigger='triggerGaMMa/'

                    counterZip=0
                    with Timer() as aggregate_time:
                        #To save each of our sample_chunks across those 2 buckets
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

                            #5/19/23 RT UPDATE: Don't need gamma trigger file anymore, since our lambda fn is phased into here for the pick querying
                            # if counterZip==1: #Only save a zip file to our new S3 bucket only once, to time a 30s increment
                            #     #To save our text file into the new bucket we have created (5/8/23 RT update) [only a sample_chunked_0.csv]
                            #     file_format_picks='triggerGamma.txt'

                            #     #To create the file
                            #     with open(file_format_picks, 'w') as fp:
                            #         pass

                            #     s3_gamma_outputfile=s3_gamma_trigger+file_format_picks #for our S3 bucket
                            #     s3.upload_to_s3(file_format_picks, s3_gamma_outputfile) 
                            #     # s3_gamma_outputfile=s3_gamma_trigger+counter_used
                            #     # s3.upload_to_s3(desired_zip_file, s3_gamma_outputfile) 

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

                    #4/7/23 RT update: (Want to print out missing stations we had that are somewhat close to what we would expect but ended up not enough)
                    num_close_stations=len(missing_waveform_totlist)
                    logger.info(
                        'waveform.closethreechannel.stations',
                        num_close_stations=str(num_close_stations),
                        waveform_closethreechannel_list=str(missing_waveform_totlist)
                    )

                    # logger.info(
                    #     'waveform.nothreechannel.stations',
                    #     waveform_nothreechannel_list=str(missing_waveform_totlist)
                    # )

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

                    logger.info(
                        'runtime.global.information',
                        latestTS=str(endtime),
                        presentTime=self.present_time,
                        currentTS=str(self.starttime),
                        uniqueStations_process_time=get_unique_channels_time_elapsed,
                        channelsFound=unique_station_str,
                        channelsNumberOfEach=count_unique_station_str
                    )


                    #3/17/23 at the end of all this, we expire all waveform data that we are ingesting that is at least 60 seconds old, to keep our nodes fresh
                    with Timer() as expire_time:
                        all_query_results=self.querymechPickRun.expire()

                    #after expiration, we do a memory check for each node
                    with Timer() as memoryAfterExpir_time:
                        string_memAfterExpir=self.querymechPickRun.get_memory_usage()

                    expire_time_elapsed=round(expire_time.elapsed,3)
                    memoryAtBegin_time_elapsed=round(memoryAtBegin_time.elapsed,3)
                    memoryAfterExpir_time_elapsed=round(memoryAfterExpir_time.elapsed,3)


                    #5/19/23 RT added: QUERYING PICK DATA FROM THE DB, FOR GaMMa USE 
                    with Timer() as access_redis_time:
                        #Strategy derived from: ConstantQueryingRedisPicks.py; DON'T NEED THIS: pickdata
                        max_time_found=self.querypickPickRun.get_max_timestamp_query()

                        if not max_time_found: #In our test, if no data is coming through, then we know we are sleeping through it. Keep it here at the moment.
                            max_time_found=0

                        #I want to wait a bit to accrue picks from the 2m5s to 1m35s mark, see if I can maximize pick acquisition here
                        #we get the a 60 second window of picks with a 30-second overlap between them 
                        initial_starttime=max_time_found-155 #can introduce a 5 second latency for pick acquisition, much like we do with the waveforms 
                        final_starttime=max_time_found-95
                        all_query_results=self.querypickPickRun.run_rt_query(initial_starttime,final_starttime)
                        queried_results=len(all_query_results)

                    querying_time=round(access_redis_time.elapsed,3)

                    with Timer() as orgpick_runGaMMa:
                        picks_fromredis_file='PicksFound.json' #will save this as a json file
                        if queried_results!=0:
                            '''
                            Reading in data that looks like this:
                            {'id': 'CI.CRF..HN', 'sta': 'CRF', 'net': 'CI', 'inst': 'HN', 'loc': '--', 'datetime': '2022-06-01T23:24:54.370', 'timestamp': 1654125894.37, 
                            'confidence': 0.3247925639152527, 'amp': 2.952020258817356e-05, 'pickwidth': 0, 'type': 'P'}
                            '''
                            #STEP 2: Organize the picks into the format to run GaMMa upon, if picks are found from query
                            list_dicts=[ast.literal_eval(eq_query) for eq_query in all_query_results]
                            data_used=pd.DataFrame(list_dicts) 
                            data_used = data_used.rename(columns={"confidence": "prob"}) #this was used in our prior "to GaMMa code"; need it as prob
                            #5/12/23 update: In the event we really get a few duplicates, let's just make sure we are not getting those (drop prob value as renamed now)
                            # df_cands = data_used[['id','datetime','prob']].drop_duplicates()
                            # df_cands = data_used.drop_duplicates() #can run this
                            df_cands = data_used.drop_duplicates(subset = ['id','datetime','prob'],keep = 'last').reset_index(drop = True)

                            chunked_data=df_cands['chunk_used'].tolist()
                            retrieved_timestamps=df_cands['retrieved_timestamp'].tolist()

                            #STEP 3: SAVE PICKS TO S3 BUCKET
                            number_picks_processed=len(df_cands)
                            if number_picks_processed!=0:
                                picks_filtered= df_cands.to_dict() #convert back to dict
                                # df_json_str = json.dumps(picks_filtered) #this is of type str now (I think this works in python3.6 and beyond, which we have)

                                #This is the point where we dump the pick str into a json file that we put back into S3
                                #RT 5/17/23, save as a json file locally in our /tmp file and then into our S3 bucket, so we can ping another lambda function to run GaMMa on it.
                                #derived from ryanTest's pandasToDict.py
                                with open(picks_fromredis_file, "w") as outfile:
                                    json.dump(picks_filtered, outfile)
                                
                                # create_new_event_folder(s3_folder_name_used) #Don't think I need to create a new S3 bucket folder
                                s3_output_file='RedisPicks/PicksFound.json'
                                s3.upload_to_s3(picks_fromredis_file, s3_output_file) 
                                #Our next lambda function will trigger off json files put into the "RedisPicks" path, retrieve the data, and will get the catalog data off of it
                            
                            #If we get picks, we report this more detailed log/otherwise we report a simpler log
                            #detailed log has unique_chunked_data_from_picks, unique_timestamped_data_from_picks,unique_picks_found
                            #dedicated logger for pick querying results 
                            logger.info(
                                'pickquery.results',
                                pick_initQueryTime=str(initial_starttime),
                                pick_finalQueryTime=str(final_starttime),
                                queryDB_picks_time=str(querying_time),
                                unique_chunked_data_from_picks=str(list(set(chunked_data))),
                                unique_timestamped_data_from_picks=str(list(set(retrieved_timestamps))),
                                unique_picks_found=str(number_picks_processed)
                            )
                        else:
                            logger.info(
                                'pickquery.results',
                                pick_initQueryTime=str(initial_starttime),
                                pick_finalQueryTime=str(final_starttime),
                                queryDB_picks_time=str(querying_time),
                                unique_picks_found="No picks in current inset"
                            )                            
                        
                        #STEP 4: All Redis based, so expire all but last 5 minutes of picks that we see 
                        #At the end of those 30 seconds, as it queries those picks, no matter what we delete picks older than 5 minutes from the max timestamp
                        self.querypickPickRun.expire() #only keep the last 5 minutes of picks that we see
                        string_memAfterClear=self.querypickPickRun.get_memory_usage()

                    get_stations_time_immed_elapsed=round(orgpick_runGaMMa.elapsed,3) #0.003
                    # print("Time ran (subset), organizing pick data: "+str(get_stations_time_immed_elapsed))

                    #to compare if we are cleaning up the memory
                    logger.info(
                        'memorycheck.results',
                        memory_atBegin=string_memAtBegin,
                        memory_afterExpir=string_memAfterExpir,
                        memory_afterExpirPicks=string_memAfterClear
                    )

                    #1/17/23 RT update: Want to add the querying information: gbytes_scanned_thirty,gbytes_metered_thirty,cost_for_query_thirty
                    logger.info(
                        'runtime.globalquery.results',
                        total_process_time=alltime_elapsed,
                        batch_time=batch_time_elapsed,
                        expire_time=expire_time_elapsed,
                        organize_save_picks_fromquery_time=get_stations_time_immed_elapsed,
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
