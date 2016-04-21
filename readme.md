# Automated Integration Testing of Spark applications with Docker, Maven and Jenkins

## Introduction

One of the problems with integration testing is that the integration tests are usually run against a shared environment.
This makes it hard for the tests to ensure that the environment is suitable for the tests to be run.
An environment where developers are working is constantly changing and therefore the consistency of such an enviornment
cannot be guaranteed.

Therefore it is desirable that we have an environment which is created just for running integration tests.
This environment can closely memic the production/qa systems and can still be in a prestine state.

Here we attempt to create such an integration testing environment by using FailSafe plugin and Docker

## Fail Safe Plugin

This plugin gives us 3 new phases in the maven build life cycle

1. pre-integration test
2. integration-test
3. post-integration-test

We can execute various goals in these phases. the task of the goals would be to
create a docker container with spark in the pre-integration test and execute the spark job.

The integration test can look at the HDFS file sytem (using Web HDFS) and run assertions on the data produced.

The post-integration test can be used to delete the docker containers.

## Maven Docker Plugin

In this project we are using the maven docker plugin plublished by

[Maven Docker Plugin by wouterd](https://github.com/wouterd/docker-maven-plugin)

This plugin is used to execute a docker file to build the image and then
copy input files into the image

The plugin is also used to launch a container during the testing phase.

## workflow

There are couple of moving parts in the project. First the maven project build kicks off.
this build invokes the docker file using the maven docker plugin.

The docker file launches a "shell script". this shell script is resonsible to copy all
the files from the container into HDFS.

The shell script then has an infinite loop to keep the container alive so that
the tests can be run against HDFS.

One tricky aspect is how does the integration test know which IP address and ports to use to query Web HDFS

this is simplified by

>   &lt;systemPropertyVariables&gt;
    &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;webhdfsurl&gt;http://${docker.containers.myapp.ports.50070/tcp.host}:${docker.containers.myapp.ports.50070/tcp.port}&lt;/webhdfsurl&gt;
    &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;datanodeurl&gt;http://${docker.containers.myapp.ports.50075/tcp.host}:${docker.containers.myapp.ports.50075/tcp.port}&lt;/datanodeurl&gt;
    &lt;/systemPropertyVariables&gt;

This way we can tell the plugin to dynamically determine which is the port which has been exposed for the actual 50070, 50075 ports and then use them at runtime.


Once the integration test receives the IP address and the port of the WebHDFS REST Service, querying the web hdfs and running validations against the output produced by the spark job is easy.

