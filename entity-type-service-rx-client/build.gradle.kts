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
  api("io.reactivex.rxjava3:rxjava:3.0.11")

  implementation("org.hypertrace.core.grpcutils:grpc-client-utils:0.12.6")
  implementation("org.hypertrace.core.grpcutils:grpc-client-rx-utils:0.12.6")

  implementation("org.slf4j:slf4j-api:1.7.30")
  implementation("com.google.guava:guava:32.1.2-jre")

  testImplementation("org.junit.jupiter:junit-jupiter:5.7.1")
  testImplementation("org.mockito:mockito-core:3.8.0")
  testImplementation("org.mockito:mockito-junit-jupiter:3.8.0")
  testImplementation("io.grpc:grpc-core:1.57.2")
  testRuntimeOnly("org.apache.logging.log4j:log4j-slf4j2-impl:2.20.0")
}
