FROM python:3.7-slim
MAINTAINER Greg Kiar <gkiar.github.io>

RUN apt-get update && apt-get install -y git wget tar docker python-numpy
RUN apt-get install -y gcc
RUN mkdir -p /clowdata /clowtask /opt

COPY requirements.txt /opt/requirements.txt
COPY ./ /src/clowdr/
RUN pip install psutil
RUN pip install -e /src/clowdr/

ENTRYPOINT ["clowdr"]
