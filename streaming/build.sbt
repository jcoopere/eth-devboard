name := "iiot-demo"

version := "1.0"

scalaVersion := "2.10.5"

val sparkVersion = "1.6.0"
val kuduVersion = "1.2.0-cdh5.10.0"

// protocol buffer support
//PB.protobufSettings
//PB.targets in Compile := Seq(
//  scalapb.gen() -> (sourceManaged in Compile).value
//)

libraryDependencies ++= Seq(
	"org.scalatest" %% "scalatest" % "2.2.1" % "test",
	"com.trueaccord.scalapb" %% "sparksql-scalapb" % "0.1.8",
	"org.apache.spark" %% "spark-core" % sparkVersion,
	"org.apache.spark" %% "spark-mllib" % sparkVersion,
	"org.apache.spark" %% "spark-streaming" % sparkVersion,
	"org.apache.spark" %% "spark-streaming-kafka" % sparkVersion,
	"org.apache.spark" %% "spark-sql" % sparkVersion,
	"org.apache.kudu" % "kudu-spark_2.10" % kuduVersion,
	"org.apache.kudu" % "kudu-client" % kuduVersion,
	"org.eclipse.paho" % "org.eclipse.paho.client.mqttv3" % "1.2.0"
)



PB.targets in Compile := Seq(
  scalapb.gen() -> (sourceManaged in Compile).value
)

PB.protocVersion := "-v261"


resolvers ++= Seq(
	"Cloudera Repository" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
	"scala-tools" at "https://oss.sonatype.org/content/groups/scala-tools",
	"Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/",
	"Second Typesafe repo" at "http://repo.typesafe.com/typesafe/maven-releases/",
	"Mesosphere Public Repository" at "http://downloads.mesosphere.io/maven",
	"Bintray sbt plugin releases" at "http://dl.bintray.com/sbt/sbt-plugin-releases/",
	"MQTT Repository" at "https://repo.eclipse.org/content/repositories/paho-releases/",
	Resolver.sonatypeRepo("public")
)

assemblyMergeStrategy in assembly <<= (assemblyMergeStrategy in assembly) { (old) =>
	{
		case m if m.toLowerCase.endsWith("manifest.mf") => MergeStrategy.discard
		case m if m.startsWith("META-INF") => MergeStrategy.discard
		case PathList("javax", "servlet", xs @ _*) => MergeStrategy.first
		case PathList("org", "apache", xs @ _*) => MergeStrategy.first
		case PathList("org", "jboss", xs @ _*) => MergeStrategy.first
		case "about.html"  => MergeStrategy.rename
		case "reference.conf" => MergeStrategy.concat
		case _ => MergeStrategy.first
	}
}

assemblyShadeRules in assembly := Seq(
	ShadeRule.rename("io.netty.handler.**" -> "shadeio.io.netty.handler.@1").inAll,
	ShadeRule.rename("io.netty.channel.**" -> "shadeioi.io.netty.channel.@1").inAll,
	ShadeRule.rename("io.netty.util.**" -> "shadeio.io.netty.util.@1").inAll,
	ShadeRule.rename("io.netty.bootstrap.**" -> "shadeio.io.netty.bootstrap.@1").inAll,
	ShadeRule.rename("com.google.common.**" -> "shade.com.google.common.@1").inAll,
	ShadeRule.rename("com.google.protobuf.**" -> "shadeproto.@1").inAll
)