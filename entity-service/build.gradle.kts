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

tasks.integrationTest {
  useJUnitPlatform()
}

dependencies {
  implementation(project(":entity-service-factory"))

  implementation("org.hypertrace.core.serviceframework:platform-grpc-service-framework:0.1.62")
  implementation("org.hypertrace.core.documentstore:document-store:0.7.48")

  runtimeOnly("io.grpc:grpc-netty:1.59.1")

  // Logging
  implementation("org.slf4j:slf4j-api:1.7.30")
  runtimeOnly("org.apache.logging.log4j:log4j-slf4j-impl:2.17.1")

  // Config
  implementation("com.typesafe:config:1.4.1")

  // integration test
  integrationTestImplementation(project(":entity-service-client"))
  integrationTestImplementation(project(":entity-service-impl"))
  integrationTestImplementation("org.hypertrace.core.grpcutils:grpc-context-utils:0.12.6")
  integrationTestImplementation("org.junit.jupiter:junit-jupiter:5.7.1")
  integrationTestImplementation("org.hypertrace.core.serviceframework:integrationtest-service-framework:0.1.62")
  integrationTestImplementation("org.testcontainers:testcontainers:1.16.1")
  integrationTestImplementation("com.github.stefanbirkner:system-lambda:1.2.0")
  integrationTestImplementation("org.hypertrace.core.attribute.service:attribute-service-api:0.13.6")
  integrationTestImplementation("org.hypertrace.core.attribute.service:attribute-service-client:0.12.5")
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

tasks.integrationTest {
  useJUnitPlatform()
  dependsOn("startAttributeServiceContainer")
  finalizedBy("stopAttributeServiceContainer")
}

tasks.register<DockerStartContainer>("startMongoContainer") {
  dependsOn("createMongoContainer")
  targetContainerId(tasks.getByName<DockerCreateContainer>("createMongoContainer").containerId)
}

tasks.register<DockerCreateContainer>("createMongoContainer") {
  dependsOn("createIntegrationTestNetwork")
  dependsOn("pullMongoImage")
  targetImageId(tasks.getByName<DockerPullImage>("pullMongoImage").image)
  containerName.set("mongo-local")
  hostConfig.network.set(tasks.getByName<DockerCreateNetwork>("createIntegrationTestNetwork").networkId)
  hostConfig.portBindings.set(listOf("37017:27017"))
  hostConfig.autoRemove.set(true)
}

tasks.register<DockerPullImage>("pullMongoImage") {
  image.set("mongo:4.4.0")
}

tasks.register<DockerStopContainer>("stopMongoContainer") {
  targetContainerId(tasks.getByName<DockerCreateContainer>("createMongoContainer").containerId)
  finalizedBy("removeIntegrationTestNetwork")
}

tasks.register<DockerCreateNetwork>("createIntegrationTestNetwork") {
  networkId.set("entity-svc-int-test")
  networkName.set("entity-svc-int-test")
}

tasks.register<DockerRemoveNetwork>("removeIntegrationTestNetwork") {
  networkId.set("entity-svc-int-test")
}

tasks.register<DockerStartContainer>("startAttributeServiceContainer") {
  dependsOn("startMongoContainer")
  dependsOn("createAttributeServiceContainer")
  targetContainerId(tasks.getByName<DockerCreateContainer>("createAttributeServiceContainer").containerId)
}

tasks.register<DockerCreateContainer>("createAttributeServiceContainer") {
  dependsOn("pullAttributeServiceImage")
  targetImageId(tasks.getByName<DockerPullImage>("pullAttributeServiceImage").image)
  containerName.set("attribute-service-local")
  envVars.put("SERVICE_NAME", "attribute-service")
  envVars.put("mongo_host", tasks.getByName<DockerCreateContainer>("createMongoContainer").containerName)
  envVars.put("BOOTSTRAP_CONFIG_URI", "file:///app/resources/configs")
  envVars.put("CLUSTER_NAME", "test")
  exposePorts("tcp", listOf(9012))
  hostConfig.portBindings.set(listOf("9112:9012"))
  hostConfig.binds.put("$projectDir/src/integrationTest/resources/configs/attribute-service/application.conf", "/app/resources/configs/attribute-service/test/application.conf")
  hostConfig.network.set(tasks.getByName<DockerCreateNetwork>("createIntegrationTestNetwork").networkId)
  hostConfig.autoRemove.set(true)
}

tasks.register<DockerPullImage>("pullAttributeServiceImage") {
  image.set("hypertrace/attribute-service:0.13.5")
}

tasks.register<DockerStopContainer>("stopAttributeServiceContainer") {
  targetContainerId(tasks.getByName<DockerCreateContainer>("createAttributeServiceContainer").containerId)
  finalizedBy("stopMongoContainer")
}
