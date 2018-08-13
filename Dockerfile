FROM alpine:edge
MAINTAINER Greg Kiar <gkiar.github.io>

RUN apk update && apk add --update bash python3-dev git wget tar docker build-base linux-headers
RUN mkdir -p /clowdata /clowtask /opt

RUN pip3 install --upgrade pip && pip3 install numpy==1.14.3
RUN pip3 install clowdr==0.0.13rc0

ENTRYPOINT ["clowdr"]
