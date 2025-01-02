from datetime import datetime, timedelta
import subprocess

start_date = datetime(2024, 12, 18)
end_date = datetime.now()
current_date = start_date

while current_date <= end_date:
    print(f"\nProcessing data for {current_date.date()}")
    
    # Run the Cloud Run job for this date
    command = f"gcloud run jobs execute analytics-processor-job --region=europe-west3"
    
    try:
        subprocess.run(command, shell=True, check=True)
        print(f"✓ Successfully processed {current_date.date()}")
    except subprocess.CalledProcessError as e:
        print(f"✗ Failed to process {current_date.date()}: {str(e)}")
    
    # Move to next day
    current_date += timedelta(days=1)