name := "httplog-analysis-view"

mainClass in assembly := Some("com.mime.bdp.Bootstrap")

assemblyJarName in assembly := s"${name.value}-${version.value}.jar"

test in assembly := {}

assemblyMergeStrategy in assembly := {
  case PathList("org", "apache", xs@_*) => MergeStrategy.first
  case PathList("org", "aopalliance", xs@_*) => MergeStrategy.last
  case PathList("javax", xs@_*) => MergeStrategy.first
  case PathList("com", "sun", xs@_*) => MergeStrategy.first
  case PathList("org", "slf4j", xs@_*) => MergeStrategy.first
  case PathList(ps@_*) if ps.last endsWith "overview.html" => MergeStrategy.filterDistinctLines
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}