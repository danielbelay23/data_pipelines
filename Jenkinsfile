pipeline {
    agent any

    

    environment {
        PATH = "/usr/local/bin:${env.PATH}"
        GCS_BUCKET = 'belayground_db'
        GCP_CREDENTIALS_ID = 'gcp_service_acct_proj_belayground'
        IMAGE_NAME = 'tweet-pipeline'
    }

    stages {
        stage('Build') {
            steps {
                script {
                    docker.build(IMAGE_NAME, '.')
                }
            }
        }

        stage('Run Application') {
            steps {
                script {
                    docker.image(IMAGE_NAME).run()
                }
            }
        }

        stage('Sync to GCS') {
            steps {
                withCredentials([file(credentialsId: GCP_CREDENTIALS_ID, variable: 'GCP_KEY_FILE')]) {
                    sh '''
                        docker run --rm --workdir /app \
                            -v "${pwd()}":/app \
                            -v "${GCP_KEY_FILE}":/gcp-key.json \
                            -e GOOGLE_APPLICATION_CREDENTIALS=/gcp-key.json \
                            google/cloud-sdk:latest \
                            gsutil rsync -r ./data gs://${GCS_BUCKET}/data
                    '''
                }
            }
        }
    }
}
