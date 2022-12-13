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
import logging
import time
import os

app = Flask(__name__)

import django

#briefly commented
#Testing
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'quakes2aws_datastore.settings')
django.setup()

from django.utils import timezone
from quakes2aws_datastore.logging import logger  # noqa:E402
from django.db import connection
from contexttimer import Timer
from timeout import timeout 
from datetime import timedelta


# Initialize Logger
LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

@app.route('/<random_string>')
def returnBackwardsString(random_string):
    """Reverse and return the provided URI"""
    LOGGER.info('Received a message: %s', random_string)
    return "".join(reversed(random_string))

#implemented a timeout decorator, we want our access to happen within 2 seconds, otherwise we "sense" something is wrong and get data through other means
#We saw that connection could take 40+ seconds
@timeout(0.000005)
def connect_db(connection):
    with connection.cursor() as cursor:
        #https://stackoverflow.com/questions/67678201/how-to-specify-timeout-for-a-query-using-django
        cursor.execute("SET statement_timeout = 2;") #set statement timeout here, see if it can set the timeout to 2 seconds here
        cursor.execute('SELECT endtime FROM sample_set ORDER BY "id" DESC LIMIT 1;') 
        row = cursor.fetchall() 
        cursor.close()
    
    with connection.cursor() as cursorTwo:
        cursorTwo.execute("SELECT pg_size_pretty( pg_total_relation_size('sample_set') );")
        rowTwo= cursorTwo.fetchall() 
        cursorTwo.close()
    return row,rowTwo


if __name__ == '__main__':
    #Can set a while True, time.sleep later on to simulate, want to make sure it has access to our DB
    while True:
        db_safe_flag=0
        #Sanity 
        with Timer() as establish_connection:
            dt_curr = timezone.now()
            dt = dt_curr - timedelta(seconds=300) 
            horizon = dt.timestamp()

            connection_test=connection.ensure_connection()
            # print("connection found")
            # print(connection_test)



        with Timer() as fetch_time:
            try:
                row,rowTwo=connect_db(connection)
            except:
                print("Timeout")
                db_safe_flag=1
        
        if db_safe_flag==0:
            statement="Commence getting waveforms"

            #RT 12/8/22 exception here
            try:
                latest_endtime=row[0][0] #should be of type 'float'
            except:
                print("Out of range, no latest endtime found")
                latest_endtime='N/A'

            #RT 12/8/22 exception here
            try:
                storage_size=rowTwo[0][0] #should be of type 'float'
            except:
                print("Out of range, no storage size found")
                storage_size='N/A'

        else:
            statement="Ping lambda fn that accesses alternate DB/DynamoDB for data when DB is replenishing storage"
            latest_endtime='DB replenish'
            storage_size='N/A'


        fetch_time_items_elapsed=round(fetch_time.elapsed,3)
        establishconn_items_elapsed=round(establish_connection.elapsed,3)

        logger.info(
            'information',
            connect_time=establishconn_items_elapsed,
            fetch_time=fetch_time_items_elapsed,
            latest_endtime_found=latest_endtime,
            statement_used=statement,
            storage_size=storage_size)
        
        #12/12/22 RT update: We don't want overruns on time here, but can have overruns (if the fetch time exceeds 30 seconds, then we can't sleep for a negative #,
        # this would error out)
        if fetch_time_items_elapsed<=30:
            time_sleeping=30-fetch_time_items_elapsed
            time.sleep(time_sleeping) #30-the time it took to fetch
        #otherwise, no sleeping, it goes to the next loop immediately


    # app.run(host='0.0.0.0', port=8080)