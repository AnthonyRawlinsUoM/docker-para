FROM continuumio/anaconda3

LABEL maintainer="Anthony Rawlins <anthony.rawlins@unimelb.edu.au>"

RUN apt-get update
RUN apt-get install -y build-essential
RUN apt-get install -y nano
RUN conda install -y pandas xarray simplejson numpy rasterio
RUN pip install hug -U 
RUN pip install marshmallow python-swiftclient python-keystoneclient
RUN pip install netcdf4

ADD lfmc /lfmc
ADD LFMCServer.py /
WORKDIR /FuelModels/
EXPOSE 8002
ENTRYPOINT ["hug", "-f", "/LFMCServer.py", "-p", "8002"]