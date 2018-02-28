FROM alpine:edge
MAINTAINER Greg Kiar <gkiar.github.io>

RUN apk update && apk add --update bash python3-dev git wget tar docker
RUN mkdir -p /clowdata /clowtask /opt

COPY requirements.txt /opt/requirements.txt
RUN pip3 install boutiques &&\
    pip3 install -e git+https://github.com/gkiar/clowdr.git@57edd78724a5e44e2bf0aafa1cb14a9693a2ec8b#egg=clowdr
 
ENTRYPOINT ["clowdr"]
