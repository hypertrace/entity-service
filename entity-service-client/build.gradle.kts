import org.hypertrace.gradle.publishing.License.APACHE_2_0

plugins {
  `java-library`
  id("org.hypertrace.publish-plugin")
  id("org.hypertrace.jacoco-report-plugin")
}

tasks.test {
  useJUnitPlatform()
}

dependencies {
  api(project(":entity-service-api"))
  api("com.typesafe:config:1.4.0")

  implementation("org.hypertrace.core.grpcutils:grpc-client-utils:0.1.0")
  implementation("org.slf4j:slf4j-api:1.7.30")

  testImplementation("org.junit.jupiter:junit-jupiter:5.6.2")
}
