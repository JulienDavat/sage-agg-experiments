FROM python:3.7.4-alpine3.10
# env proxy
RUN echo "@testing http://nl.alpinelinux.org/alpine/edge/testing" >>/etc/apk/repositories
RUN apk add --update --no-cache git g++ bash make curl nano
# config git because we are behind a proxy
RUN git config --global url."https://github.com/".insteadOf git@github.com:
RUN git config --global -l
# for rocksdb
RUN apk add --update --no-cache build-base linux-headers cmake bash #wget mercurial  autoconf libgflags-dev cmake bash
RUN apk add --update --no-cache zlib zlib-dev bzip2 bzip2-dev snappy snappy-dev lz4 lz4-dev zstd@testing zstd-dev@testing libtbb-dev@testing libtbb@testing
# installing latest gflags
RUN cd /tmp && \
    git clone https://github.com/gflags/gflags.git && \
    cd gflags && \
    mkdir build && \
    cd build && \
    cmake -DBUILD_SHARED_LIBS=1 -DGFLAGS_INSTALL_SHARED_LIBS=1 .. && \
    make install && \
    cd /tmp && \
    rm -R /tmp/gflags/
# Install Rocksdb
RUN cd /tmp && \
    git clone https://github.com/facebook/rocksdb.git && \
    cd rocksdb && \
    git checkout v6.1.2 && \
    make shared_lib && \
    mkdir -p /usr/local/rocksdb/lib && \
    mkdir /usr/local/rocksdb/include && \
    cp librocksdb.so* /usr/local/rocksdb/lib && \
    cp /usr/local/rocksdb/lib/librocksdb.so* /usr/lib/ && \
    cp -r include /usr/local/rocksdb/ && \
    cp -r include/* /usr/include/ && \
    rm -R /tmp/rocksdb/
# for postgres driver
RUN apk add --virtual build-deps gcc python-dev musl-dev && \
    apk add --no-cache --update postgresql-dev
# this enable the cache of requirments.txt
ADD ./server/sage-agg/requirements.txt /opt/sage-engine/
WORKDIR /opt/sage-engine/
# upgrade pip
RUN pip install --upgrade pip
# install deps
RUN pip install -r requirements.txt
# cause we are behind a proxy, uninstall hdt
# download manually hdt-cpp with curl -sLO
# unzip -qq
# install requirements and run setup.py
# install hdt globally for pip pip install -e pyHDT
RUN apk add unzip
RUN git clone https://github.com/Callidon/pyHDT.git && \
    cd pyHDT && \
    git checkout 0780a7ed100e64de3281a682613cc7bb520d285c && \
    curl -sLO https://github.com/rdfhdt/hdt-cpp/archive/v1.3.3.zip && \
    unzip -qq v1.3.3.zip && \
    pip install -r requirements.txt && \
    python setup.py install && \
    pip install -e .

COPY ./server/sage-agg/ /opt/sage-engine/
# then, install the sage-engine
RUN pip install -e .[hdt,postgres]
EXPOSE 8000
CMD [ "sage" ]
