FROM python:3.6-stretch

RUN pip install Cython==0.28.4 bq-sqoop

CMD ["bq-sqoop"]