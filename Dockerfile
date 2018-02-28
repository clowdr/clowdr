FROM alpine:edge
MAINTAINER Greg Kiar <gkiar.github.io>

RUN apk update && apk add --update bash python3-dev git wget tar docker
RUN mkdir -p /clowdata /clowtask /opt

COPY requirements.txt /opt/requirements.txt
RUN pip3 install boutiques &&\
    pip3 install clowdr

ENTRYPOINT ["clowdr"]
