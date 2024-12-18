#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# Define your variables
your_project_id="aggregatory-440306"  # Replace with your actual project ID
your_image_name="analytics-processor-job"  # Use your actual image name
your_service_name="analytics-processor-job"  # Use your service name

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
docker build -t $your_image_name .

# Step 5: Push the Docker image to Google Container Registry
echo "Pushing the Docker image to Google Container Registry..."
docker tag $your_image_name gcr.io/$your_project_id/$your_image_name
docker push gcr.io/$your_project_id/$your_image_name

# Step 6: Deploy to Cloud Run
echo "Deploying to Cloud Run..."
gcloud run deploy $your_service_name --image gcr.io/$your_project_id/$your_image_name --region europe-west3

# Step 7: Deploy to Firebase Hosting
echo "Deploying to Firebase Hosting..."
firebase deploy --only hosting

echo "Deployment completed successfully!"