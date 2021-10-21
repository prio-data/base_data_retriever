FROM prioreg.azurecr.io/prio-data/uvicorn_deployment:1.3.0
RUN sed 's/SECLEVEL=[0-9]/SECLEVEL=1/g' /etc/ssl/openssl.cnf > /etc/ssl/openssl.cnf

COPY ./requirements.txt /
RUN pip install -r requirements.txt 

COPY ./base_data_retriever/* /base_data_retriever/

ENV APP="base_data_retriever.app:app"
