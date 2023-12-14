plugins {
  `java-library`
}

dependencies {
  api("org.hypertrace.core.serviceframework:platform-grpc-service-framework:0.1.62")

  implementation(project(":entity-service-attribute-translator"))
  implementation(project(":entity-service-impl"))
  implementation(project(":entity-service-change-event-generator"))
  implementation("org.hypertrace.core.documentstore:document-store:0.7.48")
}
