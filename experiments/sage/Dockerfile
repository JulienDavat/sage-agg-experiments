FROM python:3.7.4-alpine3.10
# env proxy
RUN echo "@testing http://nl.alpinelinux.org/alpine/edge/testing" >>/etc/apk/repositories
RUN apk add --update --no-cache git g++ bash make curl nano
# config git because we are behind a proxy
RUN git config --global url."https://github.com/".insteadOf git@github.com:
RUN git config --global -l
# for postgres driver
RUN apk add --virtual build-deps gcc python-dev musl-dev && \
    apk add --no-cache --update postgresql-dev
# this enable the cache of requirments.txt
ADD ./server/sage-agg/requirements.txt /opt/sage-engine/
WORKDIR /opt/sage-engine/
# upgrade pip
RUN pip install --upgrade pip
RUN pip install pybind11
# install deps
RUN pip install -r requirements.txt

COPY ./server/sage-agg/ /opt/sage-engine/
# then, install the sage-engine
RUN pip install -e .[hdt,postgres]
EXPOSE 8000
CMD [ "sage" ]
