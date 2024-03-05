plugins {
  `java-library`
  jacoco
  id("org.hypertrace.jacoco-report-plugin")
}

dependencies {
  api(project(":entity-service-api"))
  implementation("org.hypertrace.core.grpcutils:grpc-context-utils:0.12.6")
  implementation("org.hypertrace.core.grpcutils:grpc-client-utils:0.12.6")
  implementation("org.hypertrace.core.attribute.service:caching-attribute-service-client:0.14.25")
  implementation("com.typesafe:config:1.4.2")
  implementation("org.slf4j:slf4j-api:2.0.9")

  annotationProcessor("org.projectlombok:lombok:1.18.30")
  compileOnly("org.projectlombok:lombok:1.18.30")

  testImplementation("org.junit.jupiter:junit-jupiter:5.7.1")
  testImplementation("org.mockito:mockito-core:3.8.0")
  testImplementation("org.mockito:mockito-junit-jupiter:3.8.0")
}

tasks.test {
  useJUnitPlatform()
}
