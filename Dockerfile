FROM python:3.9

WORKDIR /install
COPY requirements.txt .
RUN pip install --prefix="/install" -r requirements.txt


FROM python:3.9

COPY --from=0 /install /usr/local

RUN apt-get update -qq && apt-get upgrade -y

# install gdal
ENV DEBIAN_FRONTEND="noninteractive" TZ="Europe/Berlin"

RUN apt-get install -y libgdal-dev g++ --no-install-recommends && \
    apt-get clean -y && \
    rm -rf /var/lib/apt/lists/*

ENV CPLUS_INCLUDE_PATH=/usr/include/gdal
ENV C_INCLUDE_PATH=/usr/include/gdal

RUN pip3 install Fiona==1.8.17 Shapely==1.7.1

# install google chrome
RUN wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add -
RUN sh -c 'echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google-chrome.list'
RUN apt-get -y update
RUN apt-get install -y google-chrome-stable

# install chromedriver
RUN apt-get install -yqq unzip
RUN wget -O /tmp/chromedriver.zip http://chromedriver.storage.googleapis.com/`curl -sS chromedriver.storage.googleapis.com/LATEST_RELEASE`/chromedriver_linux64.zip
RUN unzip /tmp/chromedriver.zip chromedriver -d /usr/local/bin/

# set display port to avoid crash
ENV DISPLAY=:99
ENV AM_I_IN_A_DOCKER_CONTAINER Yes

WORKDIR /app
COPY src .
CMD ["python", "__init__.py"]
