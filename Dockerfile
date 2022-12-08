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
FROM python:3
# Set application working directory
WORKDIR /usr/src/app
COPY common-layer/dependencies /tmp/dependencies 

# Install requirements
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install /tmp/dependencies/quakes2aws.zip
RUN pip install https://s3-us-west-2.amazonaws.com/imss-code-drop/django-djunk/django-djunk-0.32.1.zip
RUN pip install https://s3-us-west-2.amazonaws.com/imss-code-drop/ads-extras/ads-extras-0.2.2.zip
RUN find . -name "*.pyc" -delete 
#to avoid bad magic number errors? https://stackoverflow.com/questions/514371/whats-the-bad-magic-number-error

# Install application
COPY app.py ./
# Run application
CMD python app.py

