package com.linkedin.gradle.build

import groovy.json.JsonBuilder
import org.apache.http.client.fluent.Request
import org.apache.http.entity.ContentType
import org.gradle.api.DefaultTask
import org.gradle.api.tasks.TaskAction
import org.jfrog.gradle.plugin.artifactory.dsl.ArtifactoryPluginConvention

class DistributeTask extends DefaultTask {

  @TaskAction
  void distributeBuild() {
    ArtifactoryPluginConvention convention = project.convention.plugins.artifactory
    def buildNumber = convention.clientConfig.info.buildNumber
    def buildName = convention.clientConfig.info.buildName
    def context = convention.clientConfig.publisher.contextUrl
    def password = convention.clientConfig.publisher.password

    if (password == null || password == "") {
      throw new IllegalArgumentException("password not set")
    }

    def body = [
        "publish"              : "true",
        "overrideExistingFiles": "false",
        "async"                : "true",
        "targetRepo"           : "maven",
        "sourceRepos"          : ["cruise-control"],
        "dryRun"               : "false"
    ]

    def bodyString = new JsonBuilder(body).toString()

    def url = "$context/api/build/distribute/$buildName/$buildNumber"
    logger.lifecycle("url {}", url)
    def response = Request.Post(url)
        .bodyString(bodyString, ContentType.APPLICATION_JSON)
        .addHeader("X-JFrog-Art-Api", password)
        .execute()
        .returnResponse()

    def bout = new ByteArrayOutputStream()
    response.getEntity().writeTo(bout)
    String errMsg = new String(bout.toByteArray())
    logger.lifecycle("Distribute Response: {} {}", response, errMsg)

    if (!Integer.toString(response.getStatusLine().getStatusCode()).startsWith("2")) {
      throw new IOException("http post failed")
    }
  }
}