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
                dir('your-subdir') {
                    sh 'mvn clean install'
                }
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
