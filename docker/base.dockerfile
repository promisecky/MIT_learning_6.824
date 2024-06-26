FROM ubuntu:18.04
WORKDIR /
RUN apt-get update && \
    apt-get install -y g++ git wget libssl-dev make

COPY sources.list /etc/apt/

RUN wget https://cmake.org/files/v3.22/cmake-3.22.1.tar.gz && \
    tar -xvzf cmake-3.22.1.tar.gz && cd cmake-3.22.1 && \
    chmod 777 ./configure && ./configure && make -j$(nproc) && make -j$(nproc) install

RUN cd lib && git clone https://github.com/zeromq/libzmq.git && \
    cd libzmq && mkdir build && cd build && cmake .. && make -j$(nproc) install && \
    cd /lib && git clone https://github.com/zeromq/cppzmq.git && \
    cd cppzmq && mkdir build && cd build && cmake .. && make -j$(nproc) install

RUN cp /usr/local/lib/libzmq.so.5 /lib && rm cmake-3.22.1.tar.gz

# 宿主机运行命令
# git clone https://github.com/tjumcw/6.824.git
# docker run -it --name=6.824 -v /home/zyf/cky/MIT6.824:/MIT6.824 --network="host" 6.824:latest


#docker exec  -u root -it 6.824 /bin/bash
