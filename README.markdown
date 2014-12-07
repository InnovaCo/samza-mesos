#Samza Mesos integration

This library allows to run Samza jobs on Mesos cluster.

##Getting started

Make sure you've installed JDK 1.7 (any flavor should do) and Maven.

To build and use this project Samza version 0.8 is required. It is currently not yet released in Maven repo.
You'll need to build it manually and publish it to your local repo:

    git clone https://github.com/apache/incubator-samza.git
    cd incubator-samza
    ./gradlew -PscalaVersion=2.10 clean publishToMavenLocal

To build this project, simply run:

    mvn clean package

This will result in jar file located in 'target' directory.

##Deploying to Marathon

Suppose your package structure includes directories:
- **bin** - containing standard Samza dist shell scripts (see hello-samza project)
- **config** - with your job configuration file(s)

A bash script to start your job locally using Marathon could look something like:

```shell
PACKAGE_PATH=$(readlink -e "path/to/built/package/sample-job.tar.gz")
curl -X POST -H "Content-type: application/json" localhost:8080/v2/apps --data @- <<- EOF
{
  "id": "job-name-0.1",
  "cmd": "cd bin && ./run-job.sh --config-factory=org.apache.samza.config.factories.PropertiesConfigFactory --config-path=file://$PWD/../config/sample-job.properties",
  "mem": 512,
  "cpus": 1.0,
  "instances": 1,
  "uris": [
    "file://$PACKAGE_PATH"
  ]
}
EOF
fi
```

##Running hello-samza on Mesos

...

##Configuration reference

..
