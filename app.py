"""
Main application file
Works with additional logging statements, test Dependabot
added secret keys
"""
import logging, os
import django
from PickRunner import PickRunner

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'quakes2aws_datastore.settings')
django.setup()

# from quakes2aws_datastore.core.models import State #We won't be using the PostgresDB so State is not important
from QueryExample import QueryExample

#REDIS used here
from rediscluster import RedisCluster

#Did a test where after the client is first loaded, it should be much faster to run continuously and ingest data back. 
#3.16.23 UPDATE:
redis = RedisCluster(startup_nodes=[{"host": "redis-cluster.1ge2d2.clustercfg.usw2.cache.amazonaws.com","port": "6379"}], decode_responses=True,skip_full_coverage_check=True)
query_example = QueryExample(redis)

# Initialize Logger
LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

if __name__ == '__main__':
    #3/28/23 RT update: I added querymech=query_example below so we can pass our query mechanism to our PickRunner.
    #The while loop exists within the PickRunner
    runner = PickRunner(
        starttime=None,
        count=None,
        no_sleep=False,
        live=not False,
        mark_as_processed=not False,
        querymech=query_example
    )
    runner.run(test=False)
