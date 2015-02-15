organization := "eu.inn"

name := "samza-mesos"

version := "0.2"

scalaVersion := "2.11.8"

scalacOptions ++= Seq(
  "-language:postfixOps",
  "-language:implicitConversions",
  "-feature",
  "-deprecation",
  "-unchecked",
  "-optimise",
  "-target:jvm-1.8",
  "-encoding", "UTF-8"
)

javacOptions ++= Seq(
  "-source", "1.8",
  "-target", "1.8",
  "-encoding", "UTF-8",
  "-Xlint:unchecked",
  "-Xlint:deprecation"
)

publishTo := Some("Innova libs repo" at "http://repproxy.srv.inn.ru/artifactory/libs-release-local")

credentials += Credentials(Path.userHome / ".ivy2" / ".innova_credentials")

resolvers ++= Seq(
  "Innova releases" at "http://repproxy.srv.inn.ru/artifactory/libs-release-local",
  "Innova ext releases" at "http://repproxy.srv.inn.ru/artifactory/ext-release-local"
)

libraryDependencies ++= Seq(
  "org.apache.samza"  %%  "samza-core"  % "0.10.0-inn3",
  "org.apache.samza"  %   "samza-api"   % "0.10.0-inn3",
  "org.apache.mesos"  %   "mesos"       % "1.0.1",
  "com.101tec"        %   "zkclient"    % "0.3",
  "org.scalatest"     %%  "scalatest"   % "2.2.1" % "test"
)
