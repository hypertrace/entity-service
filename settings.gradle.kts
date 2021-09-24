rootProject.name = "entity-service-root"

pluginManagement {
  repositories {
    mavenLocal()
    gradlePluginPortal()
    maven("https://hypertrace.jfrog.io/artifactory/maven")
  }
}

plugins {
  id("org.hypertrace.version-settings") version "0.2.0"
}

include(":entity-service-api")
include(":entity-service-change-event-api")
include(":entity-service-client")
include(":entity-service-change-event-impl")
include(":entity-service-impl")
include(":entity-service")
include(":entity-type-service-rx-client")
include(":entity-data-service-rx-client")
