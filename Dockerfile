#instructions for building the application container image
#PREVIOUS ONES USED
# FROM python:3
# # Set application working directory
# WORKDIR /usr/src/app
# # Install requirements
# COPY requirements.txt ./
# RUN pip install --no-cache-dir -r requirements.txt
# # Install application
# COPY app.py ./
# # Run application
# CMD python app.py


#USED NOW FOR THE QUAKES2AWS DB interface 
# FROM python:3  #OLDER WHICH WORKS
# FROM python:3.7.6-buster  #DOES NOT WORK
FROM python:3.8-alpine 
# Set application working directory
WORKDIR /usr/src/app
COPY common-layer/dependencies /tmp/dependencies 
COPY djangoItems /tmp/djangoItems 

#set environment variables to define the DB; these were previously set in our lambda function, and would help to establish a connection
# ENV DB_HOST="quakes2aws.c5te7lavw4oy.us-west-2.rds.amazonaws.com"
ENV DB_HOST="aurora-cluster1.c5te7lavw4oy.us-west-2.rds.amazonaws.com"
# ENV DB_HOST='quakes2awsreadreplica.c5te7lavw4oy.us-west-2.rds.amazonaws.com' 
ENV DB_NAME="datastore_test"
ENV DB_PASSWORD="alsdfkj0932ljafds"
ENV DB_USER="datastore_test_u"
ENV ARCHIVE_BUCKET_NAME="seismolab-quakes2aws-test-input"
ENV ARCHIVE_TO_S3="True"

# Install requirements
COPY requirements.txt ./
RUN pip install --upgrade pip 
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install /tmp/djangoItems/django-djunk-0.32.1.zip
RUN pip install /tmp/djangoItems/ads-extras-0.2.2.zip
RUN pip install /tmp/dependencies/quakes2aws.zip
# RUN pip install https://s3-us-west-2.amazonaws.com/imss-code-drop/django-djunk/django-djunk-0.32.1.zip
# RUN pip install https://s3-us-west-2.amazonaws.com/imss-code-drop/ads-extras/ads-extras-0.2.2.zip
RUN find . -name "*.pyc" -delete 
#to avoid bad magic number errors? https://stackoverflow.com/questions/514371/whats-the-bad-magic-number-error

# Install application
COPY app.py ./
COPY timeout.py ./
# Run application
CMD python app.py

