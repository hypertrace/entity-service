plugins {
  `java-library`
  jacoco
  id("org.hypertrace.jacoco-report-plugin")
}

dependencies {
  api(project(":entity-service-api"))
  api("org.hypertrace.core.serviceframework:service-framework-spi:0.1.28")

  annotationProcessor("org.projectlombok:lombok:1.18.18")
  compileOnly("org.projectlombok:lombok:1.18.18")

  implementation("org.hypertrace.core.documentstore:document-store:0.6.4")
  implementation("org.hypertrace.core.grpcutils:grpc-context-utils:0.7.0")
  implementation("org.hypertrace.core.grpcutils:grpc-client-utils:0.7.0")
  implementation("org.hypertrace.core.attribute.service:caching-attribute-service-client:0.12.3")
  implementation(project(":entity-type-service-rx-client"))
  implementation(project(":entity-service-change-event-generator"))

  implementation("com.google.protobuf:protobuf-java-util:3.19.1")
  implementation("com.github.f4b6a3:uuid-creator:3.5.0")
  implementation("io.reactivex.rxjava3:rxjava:3.0.11")
  implementation("com.google.guava:guava:30.1.1-jre")

  implementation("org.slf4j:slf4j-api:1.7.30")
  implementation("com.fasterxml.jackson.core:jackson-databind:2.12.2")

  testImplementation("org.junit.jupiter:junit-jupiter:5.7.1")
  testImplementation("org.mockito:mockito-core:3.8.0")
  testImplementation("org.mockito:mockito-junit-jupiter:3.8.0")
  testRuntimeOnly("org.apache.logging.log4j:log4j-slf4j-impl:2.17.1")
}

tasks.test {
  useJUnitPlatform()
}
