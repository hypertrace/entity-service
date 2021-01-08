plugins {
  `java-library`
  jacoco
  id("org.hypertrace.jacoco-report-plugin")
}

dependencies {
  api(project(":entity-service-api"))
  api("org.hypertrace.core.serviceframework:service-framework-spi:0.1.19")
  implementation("org.hypertrace.core.documentstore:document-store:0.5.0")
  implementation("org.hypertrace.core.grpcutils:grpc-context-utils:0.3.1")
  implementation(project(":entity-type-service-rx-client"))

  implementation("com.google.protobuf:protobuf-java-util:3.13.0")
  implementation("com.github.f4b6a3:uuid-creator:2.5.1")
  implementation("io.reactivex.rxjava3:rxjava:3.0.6")
  implementation("com.google.guava:guava:30.0-jre")

  implementation("org.slf4j:slf4j-api:1.7.30")
  implementation("com.fasterxml.jackson.core:jackson-databind:2.11.1")

  testImplementation("org.junit.jupiter:junit-jupiter:5.6.2")
  testImplementation("org.mockito:mockito-core:3.5.13")
  testImplementation("org.mockito:mockito-junit-jupiter:3.5.13")
  testRuntimeOnly("org.apache.logging.log4j:log4j-slf4j-impl:2.13.3")
}

tasks.test {
  useJUnitPlatform()
}
