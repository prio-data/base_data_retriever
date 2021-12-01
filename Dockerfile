FROM prioreg.azurecr.io/prio-data/uvicorn_deployment:2.1.0

COPY ./requirements.txt /
RUN pip install -r requirements.txt 

COPY ./base_data_retriever/* /base_data_retriever/
ENV GUNICORN_APP="base_data_retriever.app:app"
