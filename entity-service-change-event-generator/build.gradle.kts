plugins {
  `java-library`
  jacoco
  id("org.hypertrace.jacoco-report-plugin")
}

dependencies {
  api(project(":entity-service-api"))
  api(project(":entity-service-change-event-api"))
  api("com.typesafe:config:1.4.1")

  implementation("org.hypertrace.core.eventstore:event-store:0.1.2")
  implementation("com.google.guava:guava:30.1.1-jre")
  implementation("org.slf4j:slf4j-api:1.7.30")

  annotationProcessor("org.projectlombok:lombok:1.18.18")
  compileOnly("org.projectlombok:lombok:1.18.18")

  testImplementation("org.junit.jupiter:junit-jupiter:5.7.1")
  testImplementation("org.mockito:mockito-core:3.8.0")
  testImplementation("org.mockito:mockito-junit-jupiter:3.8.0")
  testRuntimeOnly("org.apache.logging.log4j:log4j-slf4j-impl:2.14.1")
}

tasks.test {
  useJUnitPlatform()
}
