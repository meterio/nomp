FROM node:10.19.0-stretch
RUN npm install -g npm@6.13.6

WORKDIR  /nomp
COPY . .

COPY _docker/supervisord.conf /etc/supervisor/conf.d/supervisord.conf

RUN apt-get update && apt-get install -y \
    python3-pip \
    python3-setuptools \
    python3-wheel \
    supervisor \
    vim \
    redis-server \
    && rm -rf /var/lib/apt/lists/*

RUN npm install 

EXPOSE 8088 8000 3008 3032 3256

ENTRYPOINT [ "/usr/bin/supervisord" ]