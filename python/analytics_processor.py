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
        print(f"Processing {country} data from {start_date.date()} to {end_date.date()}")
        
        # Get all documents first - don't use timestamp filtering in the query
        collection_name = f'pageViews_{country}'
        docs = self.db.collection(collection_name).get()
        print(f"Retrieved {len(list(docs))} total documents for {country}")
        
        # Use in-memory filtering since the timestamp format is inconsistent
        filtered_docs = []
        for doc in docs:
            data = doc.to_dict()
            try:
                # Use visitedTimestamp as the primary timestamp field
                if 'visitedTimestamp' in data:
                    ts_str = data['visitedTimestamp']
                    if isinstance(ts_str, str):
                        # Handle ISO format with Z timezone designator
                        ts = datetime.fromisoformat(ts_str.replace('Z', '+00:00'))
                        if start_date <= ts <= end_date:
                            filtered_docs.append(data)
            except Exception as e:
                print(f"Error parsing timestamp for document {doc.id}: {e}")
        
        print(f"Filtered to {len(filtered_docs)} documents within date range")
        
        daily_metrics = {}
        
        for data in filtered_docs:
            try:
                # Parse the timestamp string to get date
                ts_str = data['visitedTimestamp']
                visit_date = datetime.fromisoformat(ts_str.replace('Z', '+00:00')).date()
                date_str = visit_date.isoformat()
                
                if date_str not in daily_metrics:
                    daily_metrics[date_str] = {
                        'pageviews': 0,
                        'visitors': set(),
                        'total_time': 0
                    }
                
                # Increment pageviews
                daily_metrics[date_str]['pageviews'] += 1
                
                # Handle dailyId which might be "null" string
                visitor_id = 'unknown'
                if 'dailyId' in data and data['dailyId'] != "null":
                    visitor_id = str(data['dailyId'])
                
                daily_metrics[date_str]['visitors'].add(visitor_id)
                
                # Convert timeOnPage to float (handle string or numeric values)
                time_on_page = 0
                if 'timeOnPage' in data:
                    try:
                        time_on_page = float(data['timeOnPage'])
                    except (ValueError, TypeError):
                        pass
                
                daily_metrics[date_str]['total_time'] += time_on_page
            except Exception as e:
                print(f"Error processing document: {e}")
        
        # Convert sets to counts for serialization
        for date_str in daily_metrics:
            daily_metrics[date_str]['visitors'] = len(daily_metrics[date_str]['visitors'])
        
        print(f"Processed metrics for {len(daily_metrics)} days")
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