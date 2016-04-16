FROM sequenceiq/spark:1.6.0

MAINTAINER abhishek "http://www.foobar.com"

# COPY THE UBER JAR IN THE / OF THE FILE SYSTEM

ADD ./SparkIntegrationTestsAssembly.jar /
ADD ./entrypoint.sh /
ADD ./twitter.avro /

EXPOSE 50070 8088 8042 4040

RUN chmod +x /entrypoint.sh
CMD [""]
CMD ["./entrypoint.sh"]