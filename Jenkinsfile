/*
 *
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

def getRepoURL() {
  sh "git config --get remote.origin.url > .git/remote-url"
  return readFile(".git/remote-url").trim()
}

def getCommitSha() {
  sh "git rev-parse HEAD > .git/current-commit"
  return readFile(".git/current-commit").trim()
}

void setBuildStatus(String context, String message, String state) {
    // workaround https://issues.jenkins-ci.org/browse/JENKINS-38674
    repoUrl = getRepoURL()
    commitSha = getCommitSha()

    echo "Setting status ${repoUrl}:${commitSha} :: ${context} -> ${state} -- ${message}"
    echo "scm.branches: ${scm.branches}"
    echo "scm: ${scm}"
    step([
        $class: "GitHubCommitStatusSetter",
        // Repo needs to be manually specified because it tries to set
        // status on jenkins-common as well -- anything in context --
        // if we aren't specific
        reposSource: [$class: "ManuallyEnteredRepositorySource", url: repoUrl],
        // SHA needs to be manually specified because git commit info
        // isn't available via standard env vars, and statuses might
        // be set before we do the Jenkinsfile-based SCM checkout step
        // that gets commit info.
        commitShaSource: [$class: "ManuallyEnteredShaSource", sha: commitSha],
        // Log errors, default just swallows without logging anything
        errorHandlers: [[$class: 'ShallowAnyErrorHandler']],
        contextSource: [$class: "ManuallyEnteredCommitContextSource", context: context],
        statusResultSource: [ $class: "ConditionalStatusResultSource", results: [[$class: "AnyBuildResult", message: message, state: state]] ]
    ]);
}

def doValidation() {
  try {
    sh '''
      ./gradlew -PscalaVersion=$SCALA_VERSION clean compileJava compileScala compileTestJava compileTestScala \
	  spotlessScalaCheck checkstyleMain checkstyleTest spotbugsMain rat \
	  --profile --no-daemon --continue -PxmlSpotBugsReport=true \"$@\" \
	  || { echo 'Validation steps failed'; exit 1; }
    '''
  } catch(err) {
    error('Validation checks failed')
  }
}

def doTest() {
  sh '''
    ./gradlew -PscalaVersion=$SCALA_VERSION unitTest integrationTest \
        --profile --no-daemon --continue -PtestLoggingEvents=started,passed,skipped,failed "$@" \
        || { echo 'Test steps failed'; exit 1; }
  '''
}

def doStreamsTests() {
  // Verify that Kafka Streams archetype compiles
  sh '''
    ./gradlew streams:install clients:install connect:json:install connect:api:install \
         || { echo 'Could not install kafka-streams.jar (and dependencies) locally`'; exit 1; }
  '''

  sh '''
    version=`grep "^version=" gradle.properties | cut -d= -f 2` \
        || { echo 'Could not get version from `gradle.properties`'; exit 1; }
  '''

  sh '''
    cd streams/quickstart \
        || { echo 'Could not change into directory `streams/quickstart`'; exit 1; }
  '''

  // variable $MAVEN_LATEST__HOME is provided by Jenkins (see build configuration)
  sh 'mvn=$MAVEN_LATEST__HOME/bin/mvn'

  sh '''
    $mvn clean install -Dgpg.skip  \
        || { echo 'Could not `mvn install` streams quickstart archetype'; exit 1; }
  '''

  sh '''
    mkdir test-streams-archetype && cd test-streams-archetype \
        || { echo 'Could not create test directory for stream quickstart archetype'; exit 1; }
  '''

  sh '''
    echo "Y" | $mvn archetype:generate \
	-DarchetypeCatalog=local \
	-DarchetypeGroupId=org.apache.kafka \
	-DarchetypeArtifactId=streams-quickstart-java \
	-DarchetypeVersion=$version \
	-DgroupId=streams.examples \
	-DartifactId=streams.examples \
	-Dversion=0.1 \
	-Dpackage=myapps \
	|| { echo 'Could not create new project using streams quickstart archetype'; exit 1; }
  '''

  sh '''
    cd streams.examples \
        || { echo 'Could not change into directory `streams.examples`'; exit 1; }
  '''

  sh '''
    $mvn compile \
        || { echo 'Could not compile streams quickstart archetype project'; exit 1; }
  '''
}

pipeline {
  agent none
  stages {
    stage('Build') {
      parallel {
	stage('JDK 8') {
          agent { label 'ubuntu' }
	  tools {
	    jdk 'JDK 1.8 (latest)'
	  }
	  environment {
	    SCALA_VERSION=2.12
	  }
	  steps {
	    sh 'gradle -version'
	    doValidation()
            doTest()
            doStreamsTests()
	  }
	  post {
	    always {
	      junit '**/build/test-results/**/TEST-*.xml'
	    }
	  }
	}

	stage('JDK 11') {
          agent { label 'ubuntu' }
	  tools {
	    jdk 'JDK 11 (latest)'
	  }
	  environment {
	    SCALA_VERSION=2.13
	  }
	  steps {
	    sh 'gradle -version'
	    doValidation()
            doTest()
            echo 'Skipping Kafka Streams archetype test for Java 11'
	  }
	  post {
	    always {
	      junit '**/build/test-results/**/TEST-*.xml'
	    }
	  }
	}
       
	stage('JDK 14') {
          agent { label 'ubuntu' }
	  tools {
	    jdk 'JDK 14 (latest)'
	  }
	  environment {
	    SCALA_VERSION=2.13
	  }
	  steps {
	    sh 'gradle -version'
	    doValidation()
            doTest()
            echo 'Skipping Kafka Streams archetype test for Java 14'
	  }
	  post {
	    always {
	      junit '**/build/test-results/**/TEST-*.xml'
	    }
	  }
	}
      }
    }
  }
}
