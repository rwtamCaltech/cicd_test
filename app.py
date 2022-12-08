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


from quakes2aws_datastore.logging import logger  # noqa:E402
from django.db import connection
from contexttimer import Timer

# Initialize Logger
LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

@app.route('/<random_string>')
def returnBackwardsString(random_string):
    """Reverse and return the provided URI"""
    LOGGER.info('Received a message: %s', random_string)
    return "".join(reversed(random_string))

if __name__ == '__main__':
    #Can set a while True, time.sleep later on to simulate, want to make sure it has access to our DB
    while True:
        with Timer() as fetch_time:
            with connection.cursor() as cursor:
                cursor.execute('SELECT endtime FROM sample_set ORDER BY "id" DESC LIMIT 1;') 
                row = cursor.fetchall() 

                #RT 12/8/22 exception here
                try:
                    latest_endtime=row[0][0] #should be of type 'float'
                except:
                    print("Out of range, no latest endtime found")
        fetch_time_items_elapsed=round(fetch_time.elapsed,3)

        logger.info(
            'information',
            fetch_time=fetch_time_items_elapsed,
            latest_endtime_found=latest_endtime)
        
        time_sleeping=30-fetch_time_items_elapsed
        
        time.sleep(time_sleeping) #30-the time it took to fetch


    # app.run(host='0.0.0.0', port=8080)