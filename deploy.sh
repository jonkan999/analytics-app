#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# Step 1: Install Python dependencies
echo "Installing Python dependencies..."
pip install -r requirements.txt

# Step 2: Add changes to git
echo "Adding changes to git..."
git add .
git commit -m "Deploying latest changes"

# Step 3: Push changes to the repository
echo "Pushing changes to the repository..."
git push

# Step 4: Build the Docker image
echo "Building the Docker image..."
docker build -t analytics-processor-job .

# Step 5: Push the Docker image to Google Container Registry
echo "Pushing the Docker image to Google Container Registry..."
docker tag analytics-processor-job gcr.io/aggregatory-440306/analytics-processor-job
docker push gcr.io/aggregatory-440306/analytics-processor-job

# Step 6: Deploy to Cloud Run
echo "Deploying to Cloud Run..."
gcloud run deploy your-service-name --image gcr.io/aggregatory-440306/analytics-processor-job --region europe-west3

# Step 7: Deploy to Firebase Hosting
echo "Deploying to Firebase Hosting..."
firebase deploy --only hosting

echo "Deployment completed successfully!"