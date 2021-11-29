FROM prioreg.azurecr.io/prio-data/uvicorn_deployment:2.0.0
USER root
RUN sed 's/SECLEVEL=[0-9]/SECLEVEL=1/g' /etc/ssl/openssl.cnf > /etc/ssl/openssl.cnf

USER gunicorn
COPY ./requirements.txt /
RUN pip install -r requirements.txt 

COPY ./base_data_retriever/* /base_data_retriever/
ENV GUNICORN_APP="base_data_retriever.app:app"
