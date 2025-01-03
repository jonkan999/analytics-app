#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# Define your variables
your_project_id="aggregatory-440306"
your_image_name="analytics-processor-job"
your_service_name="analytics-processor-job"
your_region="europe-west3"
your_scheduler_job_name="analytics-processor-schedule"
firebase_project_id="aggregatory-running-dashboard"
your_service_account="analytics-processor-sa@aggregatory-440306.iam.gserviceaccount.com"


# Step 1: Authenticate with Google Cloud
echo "Authenticating with Google Cloud..."
gcloud auth login --quiet || { echo "Authentication failed. Exiting."; exit 1; }

# Step 2: Set the Google Cloud project
echo "Setting project to $your_project_id..."
gcloud config set project $your_project_id

# Step 3: Configure Docker to use Google Cloud credentials
echo "Configuring Docker to use Google Cloud credentials..."
gcloud auth configure-docker --quiet

# Step 4: Install Python dependencies
echo "Installing Python dependencies..."
pip install -r requirements.txt

# Step 5: Add changes to git
echo "Adding changes to git..."
git add .
git commit -m "Deploying latest changes"

# Step 6: Push changes to the repository
echo "Pushing changes to the repository..."
git push

# Step 7: Build the Docker image
echo "Building the Docker image..."
docker build -t $your_image_name .

# Step 8: Push the Docker image to Google Container Registry
echo "Pushing the Docker image to Google Container Registry..."
docker tag $your_image_name gcr.io/$your_project_id/$your_image_name
docker push gcr.io/$your_project_id/$your_image_name

# Step 9: Update the existing Cloud Run job with the new image
echo "Updating Cloud Run job..."
gcloud run jobs update $your_service_name \
    --image gcr.io/$your_project_id/$your_image_name \
    --region $your_region \
    --service-account="analytics-processor-sa@aggregatory-440306.iam.gserviceaccount.com"

# Step 10: Update Cloud Scheduler to point to the new job
echo "Updating Cloud Scheduler job..."
gcloud scheduler jobs delete $your_scheduler_job_name --location=$your_region --quiet || true

gcloud scheduler jobs create http $your_scheduler_job_name \
    --schedule="0 23 * * *" \
    --location=$your_region \
    --uri="https://${your_region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${your_project_id}/jobs/${your_service_name}:run" \
    --http-method=POST \
    --oauth-service-account-email="$your_service_account" \
    --oauth-token-scope="https://www.googleapis.com/auth/cloud-platform" \
    --message-body='{}' \
    --headers="Content-Type=application/json" \
    --description="Runs the analytics processor job daily at midnight Stockholm time"

# Step 11: Verify the scheduler job configuration
echo "Verifying scheduler job configuration..."
gcloud scheduler jobs describe $your_scheduler_job_name --location=$your_region

# Step 12: Test the Cloud Run job
echo "Testing the Cloud Run job..."
gcloud run jobs execute $your_service_name --region $your_region

# Step 13: Deploy to Firebase
echo "Deploying to Firebase..."
firebase deploy --only hosting

echo "Deployment and setup completed successfully!"