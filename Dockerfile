FROM alpine:edge
MAINTAINER Greg Kiar <gkiar.github.io>

RUN apk update && apk add --update bash python3-dev git wget tar docker
RUN mkdir -p /clowdata /clowtask /opt

# COPY requirements.txt /opt/requirements.txt
# COPY ./ /src/clowdr/
RUN pip3 install -e "git+https://github.com/gkiar/boutiques.git@117796d1436d251d8aaa1e736969ef35adec2c80#egg=boutiques&subdirectory=tools/python" &&\
    pip3 install -e "git+https://github.com/gkiar/clowdr.git@f8f9d8c4ee5dd208c90f76c1114f6e2d2492952f#egg=clowdr"

ENTRYPOINT ["clowdr"]
