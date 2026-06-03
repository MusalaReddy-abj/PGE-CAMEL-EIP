pipeline {
    agent any

    triggers {
        githubPush()
    }

    tools {
        maven 'mvn'   // must match the name configured in Jenkins → Global Tool Configuration
        jdk 'jdk17'     // same — match your configured JDK name
    }

    environment {
        MAVEN_OPTS = '-Xmx1024m'
        IMAGE_NAME = 'pge-camel-eip'
        IMAGE_TAG  = "1.0.0-SNAPSHOT-${env.BUILD_NUMBER}"
    }

    stages {
        stage('Checkout') {
            steps {
                checkout scm
            }
        }

        stage('Build') {
            when {
                branch 'main'
            }
            steps {
                dir('Kraken_CIS_Implementation') {
                    sh 'mvn clean install -DskipTests'
                }
            }
        }

        stage('Docker Build') {
             when {
                branch 'main'
            }
            steps {
                sh "docker build -t ${env.IMAGE_NAME}:${env.IMAGE_TAG} ."
                sh "docker tag ${env.IMAGE_NAME}:${env.IMAGE_TAG} ${env.IMAGE_NAME}:latest"
            }
        }

        stage('Deploy') {
            when {
                branch 'main'
            }
            steps {
                echo 'Add deploy steps here'
            }
        }
    }

    post {
        success {
            echo "Build #${env.BUILD_NUMBER} succeeded"
        }
        failure {
            echo "Build #${env.BUILD_NUMBER} failed"
        }
        always {
            // clean workspace after build to save disk space
            cleanWs()
        }
    }
}
