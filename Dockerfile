FROM alpine:edge
MAINTAINER Greg Kiar <gkiar.github.io>

RUN apk update && apk add --update bash python3-dev git wget tar docker
RUN mkdir -p /clowdata /clowtask /opt

COPY requirements.txt /opt/requirements.txt
RUN pip3 install boutiques &&\
    pip3 install -e git+https://github.com/gkiar/clowdr.git@7a66e39dead874c480f469fbb546d28788e1f6f7#egg=clowdr
 
ENTRYPOINT ["clowdr"]
