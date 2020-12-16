FROM  traceableai-docker.jfrog.io/hypertrace/entity-service:test

RUN wget https://github.com/hypertrace/javaagent/releases/download/0.6.2/hypertrace-agent-all.jar -O /javaagent.jar

ENV HT_SERVICE_NAME=ht-entity-service
ENV HT_REPORTING_ENDPOINT=http://host.docker.internal:9411/api/v2/spans

ENTRYPOINT java -agentlib:jdwp="transport=dt_socket,server=y,suspend=y,address=*:5000" -Dio.opentelemetry.javaagent.slf4j.simpleLogger.defaultLogLevel=debug -javaagent:/javaagent.jar -cp /app/resources:/app/classes:/app/libs/* org.hypertrace.core.serviceframework.PlatformServiceLauncher

# docker build -f Dockerfile . -t pavolloffay/ht-entity-service-agent-enabled:2
# docker run --rm -it -p 50061:50061 -p 50062:50062 -p 5000:5000 pavolloffay/ht-entity-service-agent-enabled:2
# docker push pavolloffay/ht-entity-service-agent-enabled:2

# grpcurl -plaintext -d '{}' localhost:50061 org.hypertrace.entity.data.service.v1.EntityDataService/getRelationships

