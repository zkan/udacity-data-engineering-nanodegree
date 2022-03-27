FROM apache/airflow:2.2.4

RUN pip install ccxt==1.66.32 \
  apache-airflow-providers-apache-spark==2.1.2

USER root

RUN apt update && apt install -y openjdk-11-jdk

ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64
RUN export JAVA_HOME
