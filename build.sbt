ThisBuild / scalaVersion     := "2.13.6"
ThisBuild / organization     := ""
ThisBuild / organizationName := ""
ThisBuild / javacOptions ++= Seq("-source", "1.8", "-target", "1.8")

name := "perspective-scala"

libraryDependencies += "ch.qos.logback"            % "logback-classic" % "1.2.3"
libraryDependencies += "me.shadaj"                 %% "scalapy-core"    % "0.5.0"

libraryDependencies += "com.typesafe.akka" %% "akka-stream"      % "2.6.16"
libraryDependencies += "com.typesafe.akka" %% "akka-actor-typed" % "2.6.16"
libraryDependencies +=  "com.typesafe.akka" %% "akka-http"        % "10.2.6"

libraryDependencies += "org.scalameta"            %% "munit"           % "0.7.27" % Test

fork := true

// import scala.sys.process._

// javaOptions += s"-Djna.library.path=${"python3-config --prefix".!!.trim}/lib"

// import scala.sys.process._
// lazy val pythonLdFlags = {
//   val withoutEmbed = "python3-config --ldflags".!!
//   if (withoutEmbed.contains("-lpython")) {
//     withoutEmbed.split(' ').map(_.trim).filter(_.nonEmpty).toSeq
//   } else {
//     val withEmbed = "python3-config --ldflags --embed".!!
//     withEmbed.split(' ').map(_.trim).filter(_.nonEmpty).toSeq
//   }
// }

// lazy val pythonLibsDir = {
//   pythonLdFlags.find(_.startsWith("-L")).get.drop("-L".length)
// }

// println("pythonLibsDir:" + pythonLibsDir)

// javaOptions += s"-Djna.library.path=$pythonLibsDir"

// -Djna.library.path=/usr/lib/x86_64-linux-gnu/

// export SCALAPY_PYTHON_LIBRARY=python3.8

javaOptions += s"-Djna.library.path=/Users/barry/opt/anaconda3/lib"

connectInput in run := true

// export PATH=/Library/Java/JavaVirtualMachines/zulu-11.jdk/Contents/Home/bin/:$PATH% 

 // export PATH=/Library/Java/JavaVirtualMachines/adoptopenjdk-11.jdk/Contents/Home/bin/:$PATH% 