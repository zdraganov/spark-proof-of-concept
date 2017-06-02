FROM coorpacademy/docker-pyspark:2.0.0-alpine

# GENERAL DEPENDENCIES
RUN apk update && \
    apk add zip

# PYTHON DEPENDENCIES
COPY requirements.txt /etc/requirements.txt
RUN pip install -r /etc/requirements.txt && \
    rm /etc/requirements.txt