FROM python:3-alpine

ARG proxy
ENV https_proxy $proxy
ENV http_proxy $proxy

RUN echo $http_proxy

WORKDIR /app
COPY . /app/ 

RUN echo pip install `echo $proxy | sed 's/\(\S\S*\)/--proxy \1/'` -r requirements.txt

RUN chmod +x tabmigrate.py

CMD ["./tabmigrate.py"]
