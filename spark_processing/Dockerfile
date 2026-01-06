FROM bde2020/spark-master:3.0.0-hadoop3.2

WORKDIR /app

RUN apk add --no-cache curl && \
    curl -o /spark/jars/postgresql-42.6.0.jar https://jdbc.postgresql.org/download/postgresql-42.6.0.jar

COPY process_traffic.py .

# We can reuse the entrypoint or just run spark-submit
CMD ["tail", "-f", "/dev/null"]
