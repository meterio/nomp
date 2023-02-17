FROM node:10.19.0-stretch
RUN npm install -g npm@6.13.6

WORKDIR  /nomp
COPY . .

RUN npm install 

