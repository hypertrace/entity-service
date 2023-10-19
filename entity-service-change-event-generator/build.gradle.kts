plugins {
  `java-library`
  jacoco
  id("org.hypertrace.jacoco-report-plugin")
}

dependencies {
  api(project(":entity-service-api"))
  api(project(":entity-service-change-event-api"))
  api(project(":entity-service-attribute-translator"))
  api("com.typesafe:config:1.4.1")

  implementation("org.hypertrace.core.eventstore:event-store:0.1.2")
  implementation("org.hypertrace.core.grpcutils:grpc-context-utils:0.12.6")
  implementation("com.google.guava:guava:32.1.2-jre")
  implementation("org.slf4j:slf4j-api:1.7.30")

  constraints {
    implementation("org.glassfish.jersey.core:jersey-common:2.34") {
      because("https://snyk.io/vuln/SNYK-JAVA-ORGGLASSFISHJERSEYCORE-1255637")
    }
    implementation("org.apache.commons:commons-compress:1.21") {
      because("Multiple vulnerabilities")
    }
    runtimeOnly("org.jetbrains.kotlin:kotlin-stdlib:1.6.21") {
      because("https://snyk.io/vuln/SNYK-JAVA-ORGJETBRAINSKOTLIN-2628385")
    }
  }

  runtimeOnly("io.confluent:kafka-protobuf-serializer")
  implementation(platform("org.hypertrace.core.kafkastreams.framework:kafka-bom:0.4.2"))

  annotationProcessor("org.projectlombok:lombok:1.18.18")
  compileOnly("org.projectlombok:lombok:1.18.18")

  testImplementation("org.junit.jupiter:junit-jupiter:5.7.1")
  testImplementation("org.mockito:mockito-core:3.8.0")
  testImplementation("org.mockito:mockito-junit-jupiter:3.8.0")
  testRuntimeOnly("org.apache.logging.log4j:log4j-slf4j-impl:2.17.1")
}

tasks.test {
  useJUnitPlatform()
}
