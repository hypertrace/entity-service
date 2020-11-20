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
  api("com.typesafe:config:1.4.0")

  implementation("org.hypertrace.core.grpcutils:grpc-client-utils:0.3.1")
  implementation("org.slf4j:slf4j-api:1.7.30")

  testImplementation("io.grpc:grpc-core:1.33.1")
  testImplementation("org.junit.jupiter:junit-jupiter:5.6.2")
  testImplementation("org.mockito:mockito-core:3.5.13")
  testImplementation("org.mockito:mockito-junit-jupiter:3.5.13")
  testImplementation("org.apache.logging.log4j:log4j-slf4j-impl:2.13.3")
}
