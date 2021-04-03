plugins {
  id("org.hypertrace.repository-plugin") version "0.2.3"
  id("org.hypertrace.ci-utils-plugin") version "0.2.0"
  id("org.hypertrace.jacoco-report-plugin") version "0.1.3" apply false
  id("org.hypertrace.publish-plugin") version "0.4.3" apply false
  id("org.hypertrace.docker-java-application-plugin") version "0.8.2" apply false
  id("org.hypertrace.docker-publish-plugin") version "0.8.2" apply false
  id("org.hypertrace.integration-test-plugin") version "0.1.3" apply false
  id("org.hypertrace.code-style-plugin") version "1.0.2" apply false
}

subprojects {
  group = "org.hypertrace.entity.service"
  pluginManager.withPlugin("org.hypertrace.publish-plugin") {
    configure<org.hypertrace.gradle.publishing.HypertracePublishExtension> {
      license.set(org.hypertrace.gradle.publishing.License.TRACEABLE_COMMUNITY)
    }
  }

  pluginManager.withPlugin("java") {
    apply(plugin = "org.hypertrace.code-style-plugin")
    configure<JavaPluginExtension> {
      sourceCompatibility = JavaVersion.VERSION_11
      targetCompatibility = JavaVersion.VERSION_11
    }
  }
}
