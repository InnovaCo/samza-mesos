import java.util.Properties

val samzaVersion = "0.10.0"

val appProperties = {
  val prop = new Properties()
  IO.load(prop, new File("project/version.properties"))
  prop
}

val commonSettings = Seq(
  organization := "eu.inn",
  version := appProperties.getProperty("version"),
  scalaVersion := "2.10.6",
  crossScalaVersions := Seq("2.10.6"),
  resolvers ++= Seq(
    "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/",
    "Sonatype Releases"  at "http://oss.sonatype.org/content/repositories/releases"
  ),
  libraryDependencies ++= Seq(
    "org.apache.samza"  %%  "samza-core"  % samzaVersion,
    "org.apache.samza"  %   "samza-api"   % samzaVersion,
    "org.apache.mesos"  %   "mesos"       % "1.0.1",
    "com.101tec"        %   "zkclient"    % "0.3",
    "org.scalatest"     %%  "scalatest"   % "2.2.1" % "test"
  ),
  publishMavenStyle := true,
  publishTo := {
    val nexus = "https://oss.sonatype.org/"
    if (isSnapshot.value)
      Some("snapshots" at nexus + "content/repositories/snapshots")
    else
      Some("releases"  at nexus + "service/local/staging/deploy/maven2")
  },
  licenses := Seq("Apache 2" -> url("http://www.apache.org/licenses/LICENSE-2.0.txt")),
  homepage := Some(url("https://github.com/innovaco/samza-mesos")),
  pomExtra := {
    <scm>
      <url>https://github.com/innovaco/samza-mesos</url>
      <connection>scm:git:git@github.com:innovaco/samza-mesos.git</connection>
    </scm>
    <developers>
      <developer>
        <id>kostassoid</id>
        <name>Konstantin Alexandroff</name>
        <url>http://inn.ru</url>
      </developer>
    </developers>
  }
)

lazy val `samza-mesos` = (project in file(".")).
  settings(commonSettings: _*).
  settings(
    name := "samza-mesos"
  )

lazy val demo = (project in file("./demo")).
  settings(commonSettings: _*).
  settings(
    libraryDependencies ++= Seq(
      "ch.qos.logback" % "logback-classic" % "1.1.7"
    )
  ).
  settings(
    name := "samza-mesos-demo"
  ).
  dependsOn(`samza-mesos`)
