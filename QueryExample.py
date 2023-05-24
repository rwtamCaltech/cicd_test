# from Constant import DATABASE_NAME, TABLE_NAME, ONE_GB_IN_BYTES, QUERY_COST_PER_GB_IN_DOLLARS
# from contexttimer import Timer
from quakes2aws_datastore.logging import logger  # noqa:E402
# from datetime import datetime
import time

#5/8/23 RT update
#class to expire all picks, and start with fresh batch at the beginning
class PickExpire:
    def __init__(self, client):
        self.client = client
        # self.paginator = client.get_paginator('query')
        self.pickdata = "PickData"

    def expire_all_picks(self):
        self.client.zremrangebyscore(self.pickdata, '-inf', int(time.time()))

    def get_max_timestamp_query(self):
        value_max=self.client.zrevrange(self.pickdata, 0, 0,withscores=True)
        try:
            max_value=value_max[0][1]
        except:
            # max_value=value_max #set to an empty list, if there is nothing that has been queried back. Otherwise we get the data's timestamp back
            max_value=0 #Set to 0, so we are forced to expire something (cannot expire a list); had an error in the past when we are monitoring ingestion in the past

        return max_value

    def run_rt_query(self,initial_starttime,final_starttime):
        try:
            list_of_dicts_returned=self.client.zrangebyscore(self.pickdata, initial_starttime, final_starttime)
            return list_of_dicts_returned
        except Exception as err:
            print("Data query exception")
            print(str(err))
            return [] #previously return 0, but this is our way to indicate an empty list as well if there is a problem

    def get_memory_usage(self):
        info_redis=self.client.info()
        string_total=''
        for key_value in info_redis.keys():
            used_memory=info_redis[key_value]['used_memory_human'] #this gets into each node and each node has its used memory 
            string_node=key_value+": "+str(used_memory)+', '
            string_total+=string_node
        return string_total #this returns the memory of each node used
    
    def expire(self):
        max_timestamp_found=self.get_max_timestamp_query()
        ts_used = int(max_timestamp_found)-300 #delete data up to the past 5 minutes
        self.client.zremrangebyscore(self.pickdata, '-inf', ts_used)



class QueryExample:
    def __init__(self, client):
        self.client = client
        # self.paginator = client.get_paginator('query')
        self.quakedata = "QuakeData"

    def get_memory_usage(self):
        info_redis=self.client.info()
        string_total=''
        for key_value in info_redis.keys():
            used_memory=info_redis[key_value]['used_memory_human'] #this gets into each node and each node has its used memory 
            string_node=key_value+": "+str(used_memory)+', '
            string_total+=string_node
        return string_total #this returns the memory of each node used
        
    #4/12/23 RT UPDATE: 
    #EXPIRE ALL DATA WITHIN THE REDIS DB when first running this, useful for when we switch from realtime to replay data, or vice-versa
    #We would shut down the ECS cluster, restart it so then this clearing of the DB can be invoked.
    def expire_all_data(self):
        self.client.zremrangebyscore(self.quakedata, '-inf', int(time.time()))


    '''
    DERIVED from ConstantQueryingRedisV2.py
    The expiration helper function is derived from: https://stackoverflow.com/questions/24105074/redis-to-set-timeout-for-a-key-value-pair-in-set
    set each member's score to its expiry time using epoch values
    '''
    def expire(self):
        #4/11/23 Now we want to delete data based on our latest timestamps that are in the data, now that we can reference older replayed timestamps
        #So we shouldn't reference the current time

        max_timestamp_found=self.get_max_timestamp_query()
        #EXPIRE DATA 2 minutes from our MAXIMUM DATA, see if it makes a difference
        #max_timestamp_found is a float, let's make it an int here.
        ts_used = int(max_timestamp_found)-120 #delete data up to the past 120 seconds (some "buffer", since we get data from the 35 second to the 5 second mark)
        self.client.zremrangebyscore(self.quakedata, '-inf', ts_used)


        # time_used=datetime.now()
        # ts_used = datetime.timestamp(time_used)-60 #delete data up to the past 60 seconds (some "buffer", since we get data from the 35 second to the 5 second mark)

        #EXPIRE DATA AFTER 2 mins, see if it makes a difference
        # ts_used = datetime.timestamp(time_used)-120 #delete data up to the past 60 seconds (some "buffer", since we get data from the 35 second to the 5 second mark)
        # self.client.zremrangebyscore(self.quakedata, '-inf', ts_used)

    '''
    Setting up the realtime query, default function is to get from 35 to 5 to get the latency-embedded 30 second window
    https://www.geeksforgeeks.org/default-arguments-in-python/
    '''
    def run_rt_query(self,initial_starttime,final_starttime):
        try:
            list_of_dicts_returned=self.client.zrangebyscore(self.quakedata, initial_starttime, final_starttime)
            return list_of_dicts_returned
        except Exception as err:
            logger.info('data_query.exception',error=str(err))
            # return 0
            return [] #previously return 0, but this is our way to indicate an empty list as well if there is a problem

    def get_max_timestamp_query(self):
        value_max=self.client.zrevrange(self.quakedata, 0, 0,withscores=True)
        try:
            max_value=value_max[0][1]
        except:
            # max_value=value_max #set to an empty list, if there is nothing that has been queried back. Otherwise we get the data's timestamp back
            max_value=0 #Set to 0, so we are forced to expire something (cannot expire a list); had an error in the past when we are monitoring ingestion in the past

        return max_value

    def get_min_timestamp_query(self):
        value_min=self.client.zrange(self.quakedata, 0, 0,withscores=True)
        try:
            min_value=value_min[0][1]
        except:
            min_value=value_min #set to an empty list, if there is nothing that has been queried back. Otherwise we get the data's timestamp back

        return min_value

