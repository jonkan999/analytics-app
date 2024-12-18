import firebase_admin
from firebase_admin import credentials, firestore
from datetime import datetime, time
import pytz

def check_todays_data():
    # Initialize Firebase (if not already initialized)
    if not firebase_admin._apps:
        cred = credentials.Certificate('keys/firestore_service_account.json')
        firebase_admin.initialize_app(cred)

    db = firestore.client()
    
    # Get today's start timestamp in UTC
    stockholm_tz = pytz.timezone('Europe/Stockholm')
    today = datetime.now(stockholm_tz).replace(hour=0, minute=0, second=0, microsecond=0)
    
    countries = ['se', 'no']
    
    print(f"\n=== Analytics Check for {today.date()} ===\n")
    
    for country in countries:
        collection_name = f'pageViews_{country}'
        
        # Query documents from today
        docs = db.collection(collection_name)\
                .where('timestamp', '>=', today)\
                .stream()
        
        # Process the documents
        pageviews = []
        unique_visitors = set()
        total_time = 0
        paths = {}
        
        for doc in docs:
            data = doc.to_dict()
            pageviews.append(data)
            unique_visitors.add(data.get('dailyId'))
            total_time += data.get('timeOnPage', 0)
            
            # Track path counts
            path = data.get('path', 'unknown')
            paths[path] = paths.get(path, 0) + 1
        
        # Sort paths by count
        sorted_paths = dict(sorted(paths.items(), key=lambda x: x[1], reverse=True))
        
        # Display results
        print(f"\n{country.upper()} Statistics:")
        print(f"Pageviews: {len(pageviews)}")
        print(f"Unique Visitors: {len(unique_visitors)}")
        if pageviews:
            print(f"Avg Time on Page: {round(total_time/len(pageviews), 2)}s")
        else:
            print("Avg Time on Page: 0s")
        
        if paths:
            print("\nPopular Paths:")
            for path, count in sorted_paths.items():
                print(f"  {path}: {count} views")
        else:
            print("\nNo paths recorded today")
        
        # Show some raw data for verification
        if pageviews:
            print("\nLatest Pageviews:")
            for view in sorted(pageviews, key=lambda x: x.get('timestamp', today), reverse=True)[:3]:
                print(f"  Path: {view.get('path')}")
                print(f"  Time: {view.get('timestamp').astimezone(stockholm_tz).strftime('%H:%M:%S')}")
                print(f"  TimeOnPage: {view.get('timeOnPage', 0)}s")
                print()

if __name__ == "__main__":
    check_todays_data()