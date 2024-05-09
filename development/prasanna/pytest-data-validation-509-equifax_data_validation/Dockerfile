# Using parent image with minimal python packages
# Using python 3.8.2

FROM python:3.8-slim

MAINTAINER Sai Kumar "<sai@propelo.ai>"

COPY . Pytest


# get the latest updates and install sudo, 
# since pytest_litmus is using sudo to install missing python packages
RUN apt-get update && apt-get install -y curl
#RUN apt-get install bzip2
RUN apt update \
    && apt install -y libmariadb-dev \
        gcc\
        python3-dev \
        libcogl-pango-dev \
        libcairo2-dev \
        libtool \
        linux-headers-amd64 \
        musl-dev \
        libffi-dev \
        libssl-dev \
        libjpeg-dev \
        zlib1g-dev
RUN python -m pip install -U pip
# Install required python modules
RUN pip install --no-cache-dir pytest
RUN pip install --no-cache-dir python-dateutil==2.8.1
RUN pip install --no-cache-dir pytest-html==2.1.1
RUN pip install --no-cache-dir pytest-metadata==1.8.0
RUN pip install --no-cache-dir pytz
RUN pip install --no-cache-dir pandas==1.5.2
RUN pip install --no-cache-dir memory_profiler
RUN pip install --no-cache-dir pympler
RUN pip install --no-cache-dir hurry
RUN pip install --no-cache-dir hurry.filesize
RUN pip install --no-cache-dir matplotlib
RUN pip install --no-cache-dir psycopg2-binary
RUN pip install --no-cache-dir pytest-xdist
RUN pip install --no-cache-dir pytest-html
RUN pip install --no-cache-dir python-dotenv
RUN pip install --no-cache-dir allure-pytest
RUN pip install --no-cache-dir future
RUN pip install --no-cache-dir voluptuous
RUN pip install --no-cache-dir pytest-forked
RUN pip install --no-cache-dir pytest-random-order
RUN pip install --no-cache-dir flatten_json
RUN pip install --no-cache-dir requests
RUN pip install --no-cache-dir PyGithub

WORKDIR Pytest

ENTRYPOINT ["bash", "scripts/test_list.sh"]

