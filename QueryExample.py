# from Constant import DATABASE_NAME, TABLE_NAME, ONE_GB_IN_BYTES, QUERY_COST_PER_GB_IN_DOLLARS
from contexttimer import Timer
from quakes2aws_datastore.logging import logger  # noqa:E402
from datetime import datetime

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
        


    '''
    DERIVED from ConstantQueryingRedisV2.py
    The expiration helper function is derived from: https://stackoverflow.com/questions/24105074/redis-to-set-timeout-for-a-key-value-pair-in-set
    set each member's score to its expiry time using epoch values
    '''
    def expire(self):
        time_used=datetime.now()
        ts_used = datetime.timestamp(time_used)-60 #delete data up to the past 60 seconds (some "buffer", since we get data from the 35 second to the 5 second mark)
        self.client.zremrangebyscore(self.quakedata, '-inf', ts_used)

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
            return 0

    #PREVIOUS VERSION OF MY CODE:
    # def run_rt_query(self,min_value=35,max_value=5):
    #     try:
    #         time_used=datetime.now()
    #         ts_used = datetime.timestamp(time_used) 

    #         min_timestamp_found=ts_used-min_value
    #         max_timestamp_found=ts_used-max_value

    #         min_experiment=min_timestamp_found
    #         max_experiment=max_timestamp_found

    #         list_of_dicts_returned=self.client.zrangebyscore(self.quakedata, min_experiment, max_experiment)
    #         return list_of_dicts_returned
    #     except Exception as err:
    #         logger.info('data_query.exception',error=str(err))
    #         return 0

    '''
    FROM: wsl ubuntu-18.04 elasticacheTutorialTestMinMaxOfData.py
    We will get the min/max timestamps from here
    '''
    def get_max_timestamp_query(self):
        value_max=self.client.zrevrange(self.quakedata, 0, 0,withscores=True)
        try:
            max_value=value_max[0][1]
        except:
            max_value=value_max #set to an empty list, if there is nothing that has been queried back. Otherwise we get the data's timestamp back

        return max_value

    def get_min_timestamp_query(self):
        value_min=self.client.zrange(self.quakedata, 0, 0,withscores=True)
        try:
            min_value=value_min[0][1]
        except:
            min_value=value_min #set to an empty list, if there is nothing that has been queried back. Otherwise we get the data's timestamp back

        return min_value

