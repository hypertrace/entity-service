plugins {
  `java-library`
  id("org.hypertrace.publish-plugin")
  jacoco
  id("org.hypertrace.jacoco-report-plugin")
}

tasks.test {
  useJUnitPlatform()
}

dependencies {
  api(project(":entity-service-api"))

  implementation("org.hypertrace.core.grpcutils:grpc-client-utils:0.3.0")
  implementation("org.hypertrace.core.grpcutils:grpc-client-rx-utils:0.3.0")

  implementation("org.slf4j:slf4j-api:1.7.30")
  implementation("com.google.guava:guava:30.0-jre")

  testImplementation("org.junit.jupiter:junit-jupiter:5.7.0")
  testImplementation("org.mockito:mockito-core:3.5.13")
  testImplementation("org.mockito:mockito-junit-jupiter:3.5.13")
  testImplementation("io.grpc:grpc-core:1.33.1")
  testRuntimeOnly("org.apache.logging.log4j:log4j-slf4j-impl:2.13.3")
}
