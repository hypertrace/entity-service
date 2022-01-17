plugins {
  java
  application
  jacoco
  id("org.hypertrace.docker-java-application-plugin")
  id("org.hypertrace.docker-publish-plugin")
  id("org.hypertrace.integration-test-plugin")
  id("org.hypertrace.jacoco-report-plugin")
}

tasks.integrationTest {
  useJUnitPlatform()
}

dependencies {
  implementation(project(":entity-service-impl"))
  implementation(project(":entity-service-change-event-generator"))

  implementation("org.hypertrace.core.grpcutils:grpc-server-utils:0.7.0")
  implementation("org.hypertrace.core.grpcutils:grpc-client-utils:0.7.0")
  implementation("org.hypertrace.core.serviceframework:platform-service-framework:0.1.28")
  implementation("org.hypertrace.core.documentstore:document-store:0.6.5")

  runtimeOnly("io.grpc:grpc-netty:1.43.1")

  // Logging
  implementation("org.slf4j:slf4j-api:1.7.30")
  runtimeOnly("org.apache.logging.log4j:log4j-slf4j-impl:2.17.1")

  // Config
  implementation("com.typesafe:config:1.4.1")

  // integration test
  integrationTestImplementation(project(":entity-service-client"))
  integrationTestImplementation("org.hypertrace.core.grpcutils:grpc-context-utils:0.7.0")
  integrationTestImplementation("org.junit.jupiter:junit-jupiter:5.7.1")
  integrationTestImplementation("org.hypertrace.core.serviceframework:integrationtest-service-framework:0.1.28")
  integrationTestImplementation("org.testcontainers:testcontainers:1.16.1")
  integrationTestImplementation("com.github.stefanbirkner:system-lambda:1.2.0")
}

application {
  mainClassName = "org.hypertrace.core.serviceframework.PlatformServiceLauncher"
}

// Config for gw run to be able to run this locally. Just execute gw run here on Intellij or on the console.
tasks.run<JavaExec> {
  jvmArgs = listOf("-Dbootstrap.config.uri=file:$projectDir/src/main/resources/configs", "-Dservice.name=${project.name}")
}

tasks.jacocoIntegrationTestReport {
  sourceSets(project(":entity-service-impl").sourceSets.getByName("main"))
  sourceSets(project(":entity-service-client").sourceSets.getByName("main"))
}

hypertraceDocker {
  defaultImage {
    javaApplication {
      port.set(50061)
    }
  }
}
