FROM curlimages/curl:latest as fetch-cert
USER root
RUN curl https://cacerts.digicert.com/DigiCertGlobalRootG2.crt.pem --output /root.crt

FROM prioreg.azurecr.io/prio-data/uvicorn_deployment:1.3.0

RUN sed 's/SECLEVEL=[0-9]/SECLEVEL=1/g' /etc/ssl/openssl.cnf > /etc/ssl/openssl.cnf

COPY ./requirements.txt /
RUN pip install -r requirements.txt 

RUN mkdir /certs
COPY --from=fetch-cert /root.crt /root/.postgresql/root.crt
COPY ./base_data_retriever/* /base_data_retriever/

ENV APP="base_data_retriever.app:app"
