# Set the base image to Ubuntu
FROM ubuntu

# Update the sources list
RUN apt-get update

# Install basic applications
RUN apt-get install -y tar git curl nano wget dialog net-tools build-essential

# Install Python and Basic Python Tools
RUN apt-get install -y python python-dev python-distribute python-pip

# Install Postgres Client
RUN apt-get install -y postgresql-contrib libpq-dev

# Get pip to download and install requirements:
RUN pip install pika psycopg2

ADD rabbit2postgres.py /rabbit2postgres.py

# Set the default command to execute    
# when creating a new container
CMD python /rabbit2postgres.py ${RABBIT_USER} ${RABBIT_PASSWORD} ${RABBIT_HOST} ${RABBIT_PORT} ${RABBIT_QUEUE} ${POSTGRES_DBNAME} ${POSTGRES_USER} ${POSTGRES_PASSWORD} ${POSTGRES_HOST}

