FROM python:3.6-stretch

RUN pip install --upgrade pip
RUN mkdir -p /app/bqsqoop
COPY . /app/bqsqoop
RUN pip install /app/bqsqoop

CMD ["bq-sqoop"]