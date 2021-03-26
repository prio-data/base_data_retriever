FROM curlimages/curl:latest as fetch-cert
USER root
RUN curl https://cacerts.digicert.com/DigiCertGlobalRootG2.crt.pem --output /root.crt

FROM python:3.8
RUN sed 's/SECLEVEL=[0-9]/SECLEVEL=1/g' /etc/ssl/openssl.cnf > /etc/ssl/openssl.cnf

COPY ./requirements.txt /
RUN pip install -r requirements.txt 

RUN mkdir /certs

COPY --from=fetch-cert /root.crt /root/.postgresql/root.crt

COPY ./base_data_retriever/* /base_data_retriever/
WORKDIR /base_data_retriever

CMD ["gunicorn","-k","uvicorn.workers.UvicornWorker","--bind","0.0.0.0:80","app:app"]
