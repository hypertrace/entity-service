plugins {
  `java-library`
  jacoco
  id("org.hypertrace.jacoco-report-plugin")
}

dependencies {
  api(project(":entity-service-api"))
  implementation("org.hypertrace.core.documentstore:document-store:0.1.0")
  implementation("org.hypertrace.core.grpcutils:grpc-context-utils:0.1.2")

  implementation("com.google.protobuf:protobuf-java-util:3.12.2")
  implementation("com.github.f4b6a3:uuid-creator:2.5.1")

  implementation("org.slf4j:slf4j-api:1.7.30")
  implementation("com.fasterxml.jackson.core:jackson-databind:2.11.1")

  testImplementation("org.junit.jupiter:junit-jupiter:5.6.2")
  testImplementation("org.mockito:mockito-core:3.3.3")
}

tasks.test {
  useJUnitPlatform()
}
