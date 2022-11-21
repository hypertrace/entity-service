import com.google.protobuf.gradle.protobuf
import com.google.protobuf.gradle.protoc

plugins {
  `java-library`
  id("com.google.protobuf") version "0.8.17"
  id("org.hypertrace.publish-plugin")
}

protobuf {
  protoc {
    artifact = "com.google.protobuf:protoc:3.18.0"
  }
}

sourceSets {
  main {
    java {
      srcDirs("build/generated/source/proto/main/java")
    }
  }
}

dependencies {
  api("com.google.protobuf:protobuf-java:3.19.1")
  api("org.apache.kafka:kafka-clients:6.0.1-ccs")
  api(project(":entity-service-api"))
}
