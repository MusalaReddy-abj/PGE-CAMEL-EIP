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
        AWS_REGION   = 'ap-south-2'             // your region
        AWS_ACCOUNT  = '811159390076'            // your AWS account ID
        ECR_REPO     = 'pge-integration-demo/pge-camel-eip'
        ECR_URL      = "${env.AWS_ACCOUNT}.dkr.ecr.${env.AWS_REGION}.amazonaws.com"
        FULL_IMAGE   = "${env.ECR_URL}/${env.ECR_REPO}:${env.IMAGE_TAG}"
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
                sh "docker build -t ${env.FULL_IMAGE} ."
                sh "docker tag ${env.FULL_IMAGE} ${env.ECR_URL}/${env.ECR_REPO}:latest"
            }
        }

        stage('ECR Push') {
            when {
                branch 'main'
            }
            steps {
                sh "docker push ${env.FULL_IMAGE}"
                sh "docker push ${env.ECR_URL}/${env.ECR_REPO}:latest"
            }
        }

         stage('Docker Cleanup') {
             when {
                branch 'main'
            }
            steps {
                sh "docker rmi ${env.FULL_IMAGE}"
                sh "docker rmi ${env.ECR_URL}/${env.ECR_REPO}:latest"
            }
        }

        stage('Deploy') {
            when {
                branch 'main'
            }
            steps {
                sh """
                    kubectl set image deployment/pge-camel-eip pge-camel-eip=${env.FULL_IMAGE} --namespace=default
                    kubectl rollout status deployment/pge-camel-eip --namespace=default
                """
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
