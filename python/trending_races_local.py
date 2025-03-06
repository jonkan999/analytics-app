import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
from datetime import datetime, timedelta

# Hardcoded configuration
CONFIG = {
    "SE": {
        "race_list_name": "loppkalender",
        "race_page_name": "loppsidor",
    },
    "NO": {
        "race_list_name": "terminliste",
        "race_page_name": "lopssider",
    },
    "DK": {
        "race_list_name": "lobekalender",
        "race_page_name": "lobsider",
    },
    "FI": {
        "race_list_name": "juoksukalenteri",
        "race_page_name": "kilpailusivut",
    },
    "EE": {
        "race_list_name": "jooksuvoistlused",
        "race_page_name": "jooksulehed",
    },
}

def extract_domain_name(path, race_page_name):
    # Split the path and extract the domain name part
    # Example: "/jooksulehed/lahte_vagilase_jooks/" -> "lahte_vagilase_jooks"
    parts = path.split('/')
    for i, part in enumerate(parts):
        if part == race_page_name and i + 1 < len(parts):
            return parts[i + 1]
    return None

def process_country_data(db, country, country_config, thirty_days_ago_str):
    race_list_name = country_config['race_list_name']
    race_page_name = country_config['race_page_name']
    
    # Query pageViews collection for the specific country
    pageviews_ref = db.collection(f'pageViews_{country.lower()}')
    
    # Apply filters for this country's configuration
    query = pageviews_ref.where('path', '>=', f'/{race_page_name}/').where('path', '<=', f'/{race_page_name}/\uf8ff')
    
    # Execute query
    results = query.get()
    
    # Group by path and count occurrences
    path_counts = {}
    
    for doc in results:
        data = doc.to_dict()
        
        # Check if referrer contains the country-specific race list name and timestamp is within 30 days
        referrer = data.get('referrer', '')
        visited_timestamp = data.get('visitedTimestamp', '')
        
        if (race_list_name in referrer and 
            visited_timestamp and 
            visited_timestamp >= thirty_days_ago_str):
            
            path = data.get('path', '')
            if path:
                path_counts[path] = path_counts.get(path, 0) + 1
    
    # Sort by count in descending order
    sorted_paths = sorted(path_counts.items(), key=lambda x: x[1], reverse=True)
    
    # Get top races
    top_paths = sorted_paths[:10] if len(sorted_paths) >= 10 else sorted_paths
    
    # Prepare data for Firestore
    races_data = []
    for path, count in top_paths:
        domain_name = extract_domain_name(path, race_page_name)
        if domain_name:
            races_data.append({
                "domain_name": domain_name,
                "last_30_days_views": count
            })
    
    # Write to Firestore
    trending_ref = db.collection('trendingRaces').document(country)
    trending_ref.set({
        'races': races_data,
        'updated_at': firestore.SERVER_TIMESTAMP
    })
    
    # Print results for verification
    print(f"\nTop trending races for {country} (last 30 days):")
    print("---------------------------------------------")
    for race in races_data:
        print(f"{race['domain_name']}: {race['last_30_days_views']} views")

def get_trending_races():
    # Initialize Firebase Admin with service account
    cred = credentials.Certificate('keys/firestore_service_account.json')
    
    # Handle case where app might already be initialized
    try:
        firebase_admin.initialize_app(cred)
    except ValueError:
        # App already initialized
        pass
    
    # Initialize Firestore client
    db = firestore.client()
    
    # Calculate date 30 days ago for filtering
    thirty_days_ago = datetime.now() - timedelta(days=30)
    thirty_days_ago_str = thirty_days_ago.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
    
    # Loop through each country configuration
    for country, country_config in CONFIG.items():
        process_country_data(db, country, country_config, thirty_days_ago_str)


if __name__ == "__main__":
    get_trending_races()