import firebase_admin
from firebase_admin import credentials, firestore
from datetime import datetime, timedelta
import pandas as pd

class AnalyticsProcessor:
    def __init__(self):
        if not firebase_admin._apps:
            # Use default credentials in Cloud Run
            firebase_admin.initialize_app()
        self.db = firestore.client()

    def process_analytics(self):
        """Main processing function"""
        countries = ['no', 'se', 'fi', 'dk', 'de', 'nl', 'be']  # Add more countries as needed
        end_date = datetime.now()
        
        # Process for both 7 and 28 day periods
        for days in [7, 28]:
            start_date = end_date - timedelta(days=days*2)  # Double the period for growth calc
            self._process_period(start_date, end_date, days, countries)

    def _process_period(self, start_date, end_date, period, countries):
        """Process analytics for a specific time period"""
        metrics = {
            'all': {'daily': {}, 'growth': {}},
            'by_country': {}
        }

        # Process each country
        for country in countries:
            country_metrics = self._process_country(country, start_date, end_date)
            metrics['by_country'][country] = country_metrics
            
            # Aggregate into 'all'
            for date, daily_data in country_metrics['daily'].items():
                if date not in metrics['all']['daily']:
                    metrics['all']['daily'][date] = {
                        'pageviews': 0,
                        'visitors': set(),
                        'total_time': 0
                    }
                metrics['all']['daily'][date]['pageviews'] += daily_data['pageviews']
                metrics['all']['daily'][date]['visitors'].update(daily_data['visitors'])
                metrics['all']['daily'][date]['total_time'] += daily_data['total_time']

        # Calculate growth metrics only if we have enough data
        days_needed = period * 2  # We need double the period for growth calculation
        
        # Check and calculate for 'all'
        if len(metrics['all']['daily']) >= days_needed:
            self._calculate_growth(metrics['all'], period)
        else:
            print(f"Insufficient data for {period}-day growth calculation (all countries). " 
                  f"Have {len(metrics['all']['daily'])} days, need {days_needed}.")
        
        # Check and calculate for each country
        for country, country_metrics in metrics['by_country'].items():
            if len(country_metrics['daily']) >= days_needed:
                self._calculate_growth(country_metrics, period)
            else:
                print(f"Insufficient data for {period}-day growth calculation ({country}). "
                      f"Have {len(country_metrics['daily'])} days, need {days_needed}.")

        # Convert sets to counts and format data
        self._format_metrics(metrics)

        # Store in Firestore
        doc_ref = self.db.collection('processed_analytics').document('latest')
        doc_ref.set({'metrics': metrics, 'updated_at': firestore.SERVER_TIMESTAMP})

    def _process_country(self, country, start_date, end_date):
        """Process data for a single country"""
        print(f"Processing data for {country} from {start_date} to {end_date}")
        
        # Don't use where clauses initially - we'll filter in memory after fetching
        # This helps us handle both string and timestamp fields
        docs = self.db.collection(f'pageViews_{country}').get()
        
        print(f"Retrieved {len(list(docs))} documents for {country}")
        
        daily_metrics = {}
        processed_count = 0
        skipped_count = 0
        
        for doc in docs:
            try:
                data = doc.to_dict()
                
                # Handle different timestamp field formats
                timestamp = None
                if 'visitedTimestamp' in data:
                    if isinstance(data['visitedTimestamp'], str):
                        # Parse string format
                        try:
                            timestamp = datetime.fromisoformat(data['visitedTimestamp'].replace('Z', '+00:00'))
                        except ValueError:
                            print(f"Could not parse timestamp string: {data['visitedTimestamp']}")
                            skipped_count += 1
                            continue
                    else:
                        # Already a timestamp object
                        timestamp = data['visitedTimestamp']
                elif 'timestamp' in data:
                    timestamp = data['timestamp']
                
                # Skip if no valid timestamp or outside our range
                if not timestamp or timestamp < start_date or timestamp > end_date:
                    skipped_count += 1
                    continue
                    
                # Convert to date string
                date = timestamp.date().isoformat()
                
                if date not in daily_metrics:
                    daily_metrics[date] = {
                        'pageviews': 0,
                        'visitors': set(),
                        'total_time': 0
                    }
                
                # Increment page view count
                daily_metrics[date]['pageviews'] += 1
                
                # Handle dailyId in various formats
                visitor_id = None
                if 'dailyId' in data:
                    if data['dailyId'] == "null" or data['dailyId'] is None:
                        visitor_id = 'unknown-' + doc.id
                    else:
                        visitor_id = data['dailyId']
                else:
                    visitor_id = 'unknown-' + doc.id
                    
                daily_metrics[date]['visitors'].add(visitor_id)
                
                # Handle timeOnPage in various formats
                time_on_page = 0
                if 'timeOnPage' in data:
                    if isinstance(data['timeOnPage'], str):
                        try:
                            time_on_page = float(data['timeOnPage'])
                        except ValueError:
                            time_on_page = 0
                    else:
                        time_on_page = data['timeOnPage']
                        
                daily_metrics[date]['total_time'] += time_on_page
                processed_count += 1
                
            except Exception as e:
                print(f"Error processing document {doc.id}: {str(e)}")
                skipped_count += 1
        
        print(f"Country {country}: Processed {processed_count} documents, skipped {skipped_count}")
        print(f"Country {country}: Found data for {len(daily_metrics)} unique days")
        
        if len(daily_metrics) == 0:
            print(f"WARNING: No data found for {country} in the specified date range")
        
        return {'daily': daily_metrics}

    def _calculate_growth(self, metrics, period):
        """Calculate rolling growth metrics"""
        dates = sorted(metrics['daily'].keys())
        
        # Additional check for minimum required dates
        if len(dates) < period * 2:
            return  # Skip growth calculation if not enough data
        
        df = pd.DataFrame(index=dates)
        df['pageviews'] = [metrics['daily'][date]['pageviews'] for date in dates]
        
        # Calculate rolling metrics
        df[f'rolling_{period}'] = df['pageviews'].rolling(period).mean()
        df[f'rolling_{period}_lag'] = df[f'rolling_{period}'].shift(period)
        
        # Calculate growth
        df['growth'] = ((df[f'rolling_{period}'] - df[f'rolling_{period}_lag']) / 
                       df[f'rolling_{period}_lag'] * 100)
        
        # Only store growth if we have valid calculations
        growth_values = df['growth'].dropna().round(2).to_dict()
        if growth_values:  # Only set if we have values
            metrics['growth'] = growth_values
        else:
            metrics['growth'] = {}  # Empty dict if no valid growth calculations

    def _format_metrics(self, metrics):
        """Format metrics for storage"""
        # Convert visitor sets to counts
        for country_data in [metrics['all']] + list(metrics['by_country'].values()):
            for date in country_data['daily']:
                visitors = country_data['daily'][date]['visitors']
                country_data['daily'][date]['unique_visitors'] = len(visitors)
                del country_data['daily'][date]['visitors']

def main():
    try:
        processor = AnalyticsProcessor()
        processor.process_analytics()
        print("Analytics processing completed successfully")
    except Exception as e:
        print(f"Error processing analytics: {str(e)}")
        raise e

if __name__ == "__main__":
    main()