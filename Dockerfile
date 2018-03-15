FROM continuumio/anaconda3

LABEL maintainer="Anthony Rawlins <anthony.rawlins@unimelb.edu.au>"

RUN apt-get update
RUN apt-get install -y build-essential
RUN conda install pandas xarray simplejson numpy
RUN pip install hug -U
RUN pip install marshmallow python-swiftclient python-keystoneclient

ADD lfmc /lfmc
ADD LFMCServer.py /

EXPOSE 8000
ENTRYPOINT ["hug", "-f", "LFMCServer.py"]