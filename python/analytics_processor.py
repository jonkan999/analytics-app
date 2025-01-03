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
        docs = self.db.collection(f'pageViews_{country}')\
            .where('timestamp', '>=', start_date)\
            .where('timestamp', '<=', end_date)\
            .get()

        daily_metrics = {}
        
        for doc in docs:
            data = doc.to_dict()
            date = data['timestamp'].date().isoformat()
            
            if date not in daily_metrics:
                daily_metrics[date] = {
                    'pageviews': 0,
                    'visitors': set(),
                    'total_time': 0
                }
            
            daily_metrics[date]['pageviews'] += 1
            daily_metrics[date]['visitors'].add(data['dailyId'])
            daily_metrics[date]['total_time'] += data.get('timeOnPage', 0)

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