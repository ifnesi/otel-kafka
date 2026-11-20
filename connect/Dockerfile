ARG CP_VERSION=8.2.0
FROM confluentinc/cp-server-connect:${CP_VERSION}

ARG ES_CONNECTOR_VERSION=latest

USER root
RUN confluent-hub install --no-prompt confluentinc/kafka-connect-elasticsearch:${ES_CONNECTOR_VERSION} \
 && chown -R appuser:appuser /usr/share/confluent-hub-components
USER appuser
