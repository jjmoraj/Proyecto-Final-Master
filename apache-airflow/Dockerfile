# Dockerfile
FROM apache/airflow:2.10.4

USER airflow


#Install requirements.txt 
COPY requirements.txt /
RUN pip install -r /requirements.txt

USER root

# Install OpenJDK-17
RUN apt update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get install -y ant && \
    apt-get clean;

# Set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64
RUN export JAVA_HOME

ENV AIRFLOW__CORE__SQL_ALCHEMY_CONN postgresql+psycopg2://admin:adminadmin@db_physical_store:5432/physical_store
ENV AIRFLOW__CORE__SQL_ALCHEMY_CONN postgresql+psycopg2://admin:adminadmin@db_online_store:5432/online_store


USER airflow