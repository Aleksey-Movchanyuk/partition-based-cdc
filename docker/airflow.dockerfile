###
# Airflow top level build args
###
ARG airflow_ref=2.3.4

# Base Image
FROM apache/airflow:${airflow_ref}
LABEL maintainer="DataToGo UK"

# Env vars
ENV PYTHONIOENCODING=utf-8
ENV LANG=C.UTF-8

# Bug fix for "sqlalchemy<2.0"
RUN pip uninstall psutil -y && \
      pip install psutil==5.9.4

# Setup extra tools
USER root
RUN apt-get update && \
      apt-get -y install telnet && \
      apt-get -y install git

# Switch to user airflow
USER airflow


COPY docker/airflow/requirements.txt /requirements.txt
COPY docker/airflow/entrypoint.sh /entrypoint.sh

# Test result
RUN airflow version


# Final sets
USER airflow
WORKDIR ${AIRFLOW_HOME}
ENTRYPOINT ["/entrypoint.sh"]
CMD ["webserver"]
