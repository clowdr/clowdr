FROM alpine:edge
MAINTAINER Greg Kiar <gkiar.github.io>

RUN apk update && apk add --update bash python3-dev git wget tar docker build-base linux-headers
RUN mkdir -p /clowtask /opt

RUN pip3 install --upgrade pip && pip3 install numpy==1.14.3

COPY ./ /opt/clowdr/
RUN pip3 install -r /opt/clowdr/requirements.txt
RUN pip3 install /opt/clowdr/

ENTRYPOINT ["clowdr"]
