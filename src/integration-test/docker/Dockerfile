FROM cloudera/quickstart

MAINTAINER abhishek "http://www.foobar.com"

# COPY THE UBER JAR IN THE / OF THE FILE SYSTEM

ADD /SparkIntegrationTestsAssembly.jar /

# CREATE INPUT DIRECTORY
CMD ["hadoop", "fs", "-mkdir", "-p", "/input"]

# COPY INPUT FILES
CMD ["hadoop", "fs", "-put", "/twitter.avro", "/input/twitter.avro"]

EXPOSE 8020:8020
EXPOSE 50070:50070
EXPOSE 50010:50010
EXPOSE 50020:50020
EXPOSE 50075:50075
EXPOSE 8030:8030
EXPOSE 8031:8031
EXPOSE 8032:8032
EXPOSE 8033:8033
EXPOSE 8088:8088
EXPOSE 8040:8040
EXPOSE 8042:8042
EXPOSE 10020:10020
EXPOSE 19888:19888
EXPOSE 11000:11000
EXPOSE 8888:8888
EXPOSE 18080:18080
EXPOSE 7077:7077

# RUN ALL SERVICES
ENTRYPOINT ["/usr/bin/docker-quickstart"]

# RUN SPARK JOB
CMD ["spark-submit", "--class", "com.abhi.HelloWorld", "--master", "local[2]", "SparkIntegrationTestsAssembly.jar", "/input", "/output"]