plugins {
  id("org.hypertrace.repository-plugin") version "0.4.2"
  id("org.hypertrace.ci-utils-plugin") version "0.3.2"
  id("org.hypertrace.jacoco-report-plugin") version "0.2.1" apply false
  id("org.hypertrace.publish-plugin") version "1.0.5" apply false
  id("org.hypertrace.docker-java-application-plugin") version "0.9.9" apply false
  id("org.hypertrace.docker-publish-plugin") version "0.9.9" apply false
  id("org.hypertrace.integration-test-plugin") version "0.2.0" apply false
  id("org.hypertrace.code-style-plugin") version "1.2.0" apply false
  id("org.owasp.dependencycheck") version "8.2.1"
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

dependencyCheck {
  format = org.owasp.dependencycheck.reporting.ReportGenerator.Format.ALL.toString()
  suppressionFile = "owasp-suppressions.xml"
  scanConfigurations.add("runtimeClasspath")
  failBuildOnCVSS = 7.0F
}
