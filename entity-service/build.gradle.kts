import com.bmuschko.gradle.docker.tasks.container.DockerCreateContainer
import com.bmuschko.gradle.docker.tasks.container.DockerStartContainer
import com.bmuschko.gradle.docker.tasks.container.DockerStopContainer
import com.bmuschko.gradle.docker.tasks.image.DockerPullImage
import com.bmuschko.gradle.docker.tasks.network.DockerCreateNetwork
import com.bmuschko.gradle.docker.tasks.network.DockerRemoveNetwork

plugins {
  java
  application
  jacoco
  id("org.hypertrace.docker-java-application-plugin")
  id("org.hypertrace.docker-publish-plugin")
  id("org.hypertrace.integration-test-plugin")
  id("org.hypertrace.jacoco-report-plugin")
}

tasks.register<DockerCreateNetwork>("createIntegrationTestNetwork") {
  networkName.set("entity-svc-int-test")
}

tasks.register<DockerRemoveNetwork>("removeIntegrationTestNetwork") {
  networkId.set("entity-svc-int-test")
}

tasks.register<DockerPullImage>("pullMongoImage") {
  image.set("mongo:4.2.0")
}

tasks.register<DockerCreateContainer>("createMongoContainer") {
  dependsOn("createIntegrationTestNetwork")
  dependsOn("pullMongoImage")
  targetImageId(tasks.getByName<DockerPullImage>("pullMongoImage").image)
  containerName.set("mongo-local")
  hostConfig.network.set(tasks.getByName<DockerCreateNetwork>("createIntegrationTestNetwork").networkId)
  hostConfig.portBindings.set(listOf("27017:27017"))
  hostConfig.autoRemove.set(true)
}

tasks.register<DockerStartContainer>("startMongoContainer") {
  dependsOn("createMongoContainer")
  targetContainerId(tasks.getByName<DockerCreateContainer>("createMongoContainer").containerId)
}

tasks.register<DockerStopContainer>("stopMongoContainer") {
  targetContainerId(tasks.getByName<DockerCreateContainer>("createMongoContainer").containerId)
  finalizedBy("removeIntegrationTestNetwork")
}

tasks.integrationTest {
  useJUnitPlatform()
  dependsOn("startMongoContainer")
  finalizedBy("stopMongoContainer")
}

dependencies {
  implementation(project(":entity-service-impl"))

  implementation("org.hypertrace.core.grpcutils:grpc-server-utils:0.1.0")
  implementation("org.hypertrace.core.serviceframework:platform-service-framework:0.1.2")
  implementation("org.hypertrace.core.documentstore:document-store:0.1.0")

  runtimeOnly("io.grpc:grpc-netty:1.30.2")

  // Logging
  implementation("org.slf4j:slf4j-api:1.7.30")
  runtimeOnly("org.apache.logging.log4j:log4j-slf4j-impl:2.13.3")

  // Config
  implementation("com.typesafe:config:1.4.0")

  integrationTestImplementation(project(":entity-service-client"))
  integrationTestImplementation("org.junit.jupiter:junit-jupiter:5.6.2")
  integrationTestImplementation("org.hypertrace.core.serviceframework:integrationtest-service-framework:0.1.2")
}

application {
  mainClassName = "org.hypertrace.core.serviceframework.PlatformServiceLauncher"
}

// Config for gw run to be able to run this locally. Just execute gw run here on Intellij or on the console.
tasks.run<JavaExec> {
  jvmArgs = listOf("-Dbootstrap.config.uri=file:${projectDir}/src/main/resources/configs", "-Dservice.name=${project.name}")
}

tasks.jacocoIntegrationTestReport {
  sourceSets(project(":entity-service-impl").sourceSets.getByName("main"))
  sourceSets(project(":entity-service-client").sourceSets.getByName("main"))
}
