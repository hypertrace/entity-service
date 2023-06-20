plugins {
  `java-library`
  jacoco
  id("org.hypertrace.publish-plugin")
  id("org.hypertrace.jacoco-report-plugin")
}

tasks.test {
  useJUnitPlatform()
}

dependencies {
  api(project(":entity-service-api"))
  api("com.typesafe:config:1.4.1")

  implementation("org.hypertrace.core.grpcutils:grpc-client-utils:0.12.0")
  implementation("org.slf4j:slf4j-api:1.7.30")
  implementation("org.hypertrace.core.serviceframework:platform-metrics:0.1.52")

  testImplementation("io.grpc:grpc-core:1.56.0")
  testImplementation("org.junit.jupiter:junit-jupiter:5.7.1")
  testImplementation("org.mockito:mockito-core:3.8.0")
  testImplementation("org.mockito:mockito-junit-jupiter:3.8.0")
  testImplementation("org.apache.logging.log4j:log4j-slf4j-impl:2.17.1")
}
