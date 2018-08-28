FROM alpine:edge
MAINTAINER Greg Kiar <gkiar.github.io>

RUN apk update && apk add --update bash python3-dev git wget tar docker build-base linux-headers
RUN mkdir -p /clowdata /clowtask /opt

RUN pip3 install --upgrade pip && pip3 install numpy==1.14.3
RUN pip3 install boto3 \
                 flask \
                 boutiques>=0.5.10 \
                 pandas>=0.23.4 \
                 slurmpy==0.0.7 \
                 dash>=0.24.1 \
                 dash-renderer>=0.13.0 \
                 dash-html-components>=0.11.0 \
                 dash-core-components>=0.27.1 \
                 dash-table-experiments>=0.6.0 \
                 colorlover \
                 plotly>=3.1.1 \
                 psutil
RUN pip3 install clowdr==0.0.17rc1

ENTRYPOINT ["clowdr"]
