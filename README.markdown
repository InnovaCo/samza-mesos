Samza on Mesos
--------------

This project allows to run Samza jobs on Mesos cluster.

##Status

Early development. Not tested in production. Hints/issues/PRs are welcome.

##Building

Make sure you've installed JDK 1.6+ (tested with Oracle JDK 1.7) and Maven.

First, Samza version 0.9.* is required to build and use this package. (If) it is not yet released in Maven repo,
you need to make sure it is available from your local repo:

    git clone https://github.com/apache/incubator-samza.git
    cd incubator-samza
    git reset --hard 377e5cc3f56549a02cbbb79cc3dc7166df7da585
    ./gradlew -PscalaVersion=2.10 clean publishToMavenLocal

Note that we're resetting to specific commit to avoid possible breaking changes.

Then you can build Samza-Mesos from sources:

    git clone https://github.com/InnovaCo/samza-mesos.git

To simply build a package, run:

    mvn clean package

This will result in a jar file located in 'target' directory.

To build and install a package to local repo, run:

    mvn clean install

After this you should be able to reference it like this:

```xml
<dependency>
  <groupId>eu.inn</groupId>
  <artifactId>samza-mesos</artifactId>
  <version>0.1.0-SNAPSHOT</version>
</dependency>
```

##Deploying jobs to Marathon

From Mesos point of view each Samza job is a framework. Although not required, it is convenient to use Marathon to host a job scheduler as another Mesos task.

Suppose your job package includes directories:
- **bin** - containing standard Samza distributed shell scripts (see [hello-samza](https://github.com/apache/incubator-samza-hello-samza))
- **config** - with your job configuration file(s)

A shell script to start your job locally using Marathon might look something like this:

```shell
#!/bin/bash
PACKAGE_PATH=$(readlink -e "path/to/built/package/sample-job-X.X.X.tar.gz")
curl -X POST -H "Content-type: application/json" localhost:8080/v2/apps --data @- <<- EOF
{
  "id": "sample-job-X.X.X",
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

##Migration from YARN

To switch from using YARN to Mesos you probably only need to edit your job .properties file (apart from including Samza-Mesos package as a dependency):

    job.factory.class=eu.inn.samza.mesos.MesosJobFactory

Rename 'yarn.package.path' to 'mesos.package.path'.

Rename 'yarn.container.count' to 'mesos.executor.count'.

Set 'mesos.master.connect' to Mesos master address. For example:

    mesos.master.connect=zk://localhost:2181/mesos

Optionally, set constraints for each executor (container). Like this:

    mesos.executor.cpu.cores=1
    mesos.executor.memory.mb=1024

For more options, see configuration reference below.

##Configuration reference

| Property                           | Default value             | Description                               |
|------------------------------------|---------------------------|-------------------------------------------|
| mesos.package.path                 |                           | Job package URI (file, http, hdfs)        |
| mesos.master.connect               | zk://localhost:2181/mesos | Mesos master URL                          |
| mesos.executor.memory.mb           | 1024                      | Executor memory constraint                |
| mesos.executor.cpu.cores           | 1                         | Executor CPU cores constraint             |
| mesos.executor.disk.mb             | 1024                      | Executor disk constraint                  |
| mesos.executor.attributes.*        |                           | Slave attributes reqs (regex expressions) |
| mesos.executor.count               | 1                         | Executor count to distribute job          |
| mesos.scheduler.user               |                           | System user for starting executors        |
| mesos.scheduler.role               |                           | Mesos role to use for this scheduler      |
| mesos.scheduler.failover.timeout   | a lot (Long.MaxValue)     | Framework failover timeout                |

##Acknowledgements

This project is based on Jon Bringhurst's [prototype](https://github.com/fintler/samza/tree/SAMZA-375/samza-mesos).
