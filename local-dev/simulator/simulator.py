#!/usr/bin/env python3
"""
NSP Bolt Ride - Complete Simulator Integration
Connects the web simulator to the local trip processor
"""

import requests
import json
import time
import csv
import random
import uuid
import os
import argparse
import logging
from datetime import datetime, timedelta
from dataclasses import dataclass
from typing import List, Dict, Optional
import threading
from concurrent.futures import ThreadPoolExecutor

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@dataclass
class CSVTripData:
    """Structure for CSV trip data"""
    trip_start: Dict
    trip_end: Dict

class TripEventGenerator:
    """Generate realistic trip events from CSV data"""
    
    def __init__(self, trip_start_csv: str, trip_end_csv: str):
        self.trip_start_data = self._load_csv(trip_start_csv)
        self.trip_end_data = self._load_csv(trip_end_csv)
        self.trip_data_map = self._correlate_trip_data()
        
    def _load_csv(self, filename: str) -> List[Dict]:
        """Load CSV file into list of dictionaries"""
        data = []
        try:
            with open(filename, 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    # Convert numeric fields
                    for key, value in row.items():
                        if key in ['pickup_location_id', 'dropoff_location_id', 'vendor_id']:
                            row[key] = int(float(value))
                        elif key in ['estimated_fare_amount', 'fare_amount', 'tip_amount', 'rate_code', 'passenger_count', 'trip_distance', 'trip_type', 'payment_type']:
                            row[key] = float(value)
                    data.append(row)
            logger.info(f"Loaded {len(data)} rows from {filename}")
            return data
        except FileNotFoundError:
            logger.error(f"CSV file not found: {filename}")
            return []
        except Exception as e:
            logger.error(f"Error loading {filename}: {e}")
            return []
    
    def _correlate_trip_data(self) -> Dict[str, CSVTripData]:
        """Correlate trip start and end data by trip_id"""
        trip_map = {}
        
        # Create lookup for trip_end data
        trip_end_lookup = {row['trip_id']: row for row in self.trip_end_data}
        
        # Correlate with trip_start data
        for start_row in self.trip_start_data:
            trip_id = start_row['trip_id']
            if trip_id in trip_end_lookup:
                trip_map[trip_id] = CSVTripData(
                    trip_start=start_row,
                    trip_end=trip_end_lookup[trip_id]
                )
        
        logger.info(f"Correlated {len(trip_map)} complete trips")
        return trip_map
    
    def generate_trip_start_event(self, base_trip_data: Optional[CSVTripData] = None) -> tuple[Dict, Optional[CSVTripData]]:
        """Generate a trip_start event"""
        if not base_trip_data and self.trip_data_map:
            base_trip_data = random.choice(list(self.trip_data_map.values()))
        elif not base_trip_data:
            # Fallback to synthetic data
            return self._generate_synthetic_trip_start(), None
        
        trip_id = f"trip_{uuid.uuid4().hex[:8]}"
        now = datetime.utcnow()
        
        # Use CSV data as template with variations
        start_data = base_trip_data.trip_start
        
        # Add geographic variance around Accra
        base_lat = 5.55
        base_lon = -0.20
        
        event = {
            "event_type": "trip_start",
            "schema_version": "2.0",
            "trip_id": trip_id,
            "user_id": f"user_{random.randint(1000, 9999)}",
            "driver_id": f"driver_{random.randint(100, 999)}",
            "pickup_location": {
                "lat": round(base_lat + (random.random() - 0.5) * 0.1, 6),
                "lon": round(base_lon + (random.random() - 0.5) * 0.1, 6)
            },
            "pickup_location_id": int(start_data.get('pickup_location_id', 93)),
            "dropoff_location_id": int(start_data.get('dropoff_location_id', 93)),
            "vendor_id": int(start_data.get('vendor_id', 1)),
            "pickup_datetime": now.isoformat() + 'Z',
            "estimated_dropoff_datetime": (now + timedelta(minutes=random.randint(15, 45))).isoformat() + 'Z',
            "estimated_fare": round(float(start_data.get('estimated_fare_amount', 25.0)) * random.uniform(0.8, 1.2), 2),
            "timestamp": now.isoformat() + 'Z',
            "metadata": {
                "source": "mobile_app_simulator",
                "version": "v3.2.1",
                "region": "accra",
                "simulator_id": os.getpid()
            }
        }
        
        return event, base_trip_data
    
    def generate_trip_end_event(self, trip_start_event: Dict, base_trip_data: CSVTripData) -> Dict:
        """Generate corresponding trip_end event"""
        now = datetime.utcnow()
        start_time = datetime.fromisoformat(trip_start_event['pickup_datetime'].replace('Z', ''))
        duration_minutes = (now - start_time).total_seconds() / 60
        
        # Use CSV data as template
        end_data = base_trip_data.trip_end
        
        # Add realistic variance to fare
        estimated_fare = trip_start_event['estimated_fare']
        fare_variance = random.uniform(-0.15, 0.25)  # -15% to +25% variance
        final_fare = max(estimated_fare * (1 + fare_variance), 5.0)  # Minimum fare
        
        event = {
            "event_type": "trip_end",
            "schema_version": "2.0", 
            "trip_id": trip_start_event['trip_id'],
            "dropoff_location": {
                "lat": round(trip_start_event['pickup_location']['lat'] + (random.random() - 0.5) * 0.05, 6),
                "lon": round(trip_start_event['pickup_location']['lon'] + (random.random() - 0.5) * 0.05, 6)
            },
            "dropoff_datetime": now.isoformat() + 'Z',
            "final_fare": round(final_fare, 2),
            "tip_amount": round(final_fare * random.uniform(0, 0.2), 2),  # 0-20% tip
            "distance_km": round(random.uniform(2, 20), 2),
            "duration_minutes": round(duration_minutes),
            "payment_type": int(end_data.get('payment_type', random.randint(1, 4))),
            "rate_code": int(end_data.get('rate_code', random.randint(1, 5))),
            "passenger_count": int(end_data.get('passenger_count', random.randint(1, 4))),
            "trip_type": int(end_data.get('trip_type', random.randint(1, 2))),
            "timestamp": now.isoformat() + 'Z',
            "metadata": {
                "source": "mobile_app_simulator",
                "version": "v3.2.1", 
                "region": "accra",
                "simulator_id": os.getpid()
            }
        }
        
        return event
    
    def _generate_synthetic_trip_start(self) -> Dict:
        """Generate synthetic trip_start when no CSV data available"""
        trip_id = f"trip_{uuid.uuid4().hex[:8]}"
        now = datetime.utcnow()
        
        return {
            "event_type": "trip_start",
            "schema_version": "2.0",
            "trip_id": trip_id,
            "user_id": f"user_{random.randint(1000, 9999)}",
            "driver_id": f"driver_{random.randint(100, 999)}",
            "pickup_location": {"lat": 5.55, "lon": -0.20},
            "pickup_location_id": 93,
            "dropoff_location_id": 95,
            "vendor_id": 1,
            "pickup_datetime": now.isoformat() + 'Z',
            "estimated_dropoff_datetime": (now + timedelta(minutes=30)).isoformat() + 'Z',
            "estimated_fare": round(random.uniform(15, 50), 2),
            "timestamp": now.isoformat() + 'Z',
            "metadata": {"source": "synthetic_generator"}
        }

class EventSender:
    """Send events to local processor API with connection pooling"""
    
    def __init__(self, api_url: str = "http://localhost:5000", max_workers: int = 10):
        self.api_url = api_url
        self.session = requests.Session()
        self.session.headers.update({'Content-Type': 'application/json'})
        self.max_workers = max_workers
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.stats = {
            'sent': 0,
            'failed': 0,
            'start_time': time.time()
        }
        
    def send_event(self, event: Dict) -> bool:
        """Send event to processor API"""
        try:
            response = self.session.post(
                f"{self.api_url}/events",
                json=event,
                timeout=10
            )
            
            if response.status_code == 200:
                self.stats['sent'] += 1
                logger.debug(f"✅ Sent {event['event_type']} for trip {event['trip_id']}")
                return True
            else:
                self.stats['failed'] += 1
                logger.error(f"❌ Failed to send event: {response.status_code} - {response.text}")
                return False
                
        except requests.exceptions.RequestException as e:
            self.stats['failed'] += 1
            logger.error(f"❌ Network error sending event: {str(e)}")
            return False
    
    def send_event_async(self, event: Dict):
        """Send event asynchronously"""
        return self.executor.submit(self.send_event, event)
    
    def check_health(self) -> bool:
        """Check if processor API is healthy"""
        try:
            response = self.session.get(f"{self.api_url}/health", timeout=5)
            return response.status_code == 200
        except:
            return False
    
    def get_metrics(self) -> Dict:
        """Get current system metrics"""
        try:
            response = self.session.get(f"{self.api_url}/metrics", timeout=5)
            if response.status_code == 200:
                return response.json()
            return {}
        except:
            return {}
    
    def get_stats(self) -> Dict:
        """Get sender statistics"""
        elapsed = time.time() - self.stats['start_time']
        return {
            **self.stats,
            'elapsed_seconds': elapsed,
            'events_per_second': self.stats['sent'] / elapsed if elapsed > 0 else 0
        }

class TripSimulator:
    """Main simulator orchestrator"""
    
    def __init__(self, trip_start_csv: str, trip_end_csv: str, api_url: str = "http://localhost:5000"):
        self.generator = TripEventGenerator(trip_start_csv, trip_end_csv)
        self.sender = EventSender(api_url)
        self.active_trips = {}  # trip_id -> (start_event, base_data, end_time)
        self.running = False
        
    def run_simulation(self, events_per_second: float = 2.0, duration_seconds: int = 300, 
                      trip_duration_range: tuple = (60, 300)):
        """Run the simulation"""
        logger.info(f"🚀 Starting simulation: {events_per_second} events/sec for {duration_seconds}s")
        
        # Check API health
        if not self.sender.check_health():
            logger.error("❌ Processor API is not healthy. Start the processor first!")
            logger.info("   Run: python src/trip_processor.py")
            return False
        
        logger.info("✅ Processor API is healthy")
        
        self.running = True
        start_time = time.time()
        event_interval = 1.0 / events_per_second
        next_stats_time = time.time() + 30  # Print stats every 30 seconds
        
        try:
            while self.running and (time.time() - start_time) < duration_seconds:
                loop_start = time.time()
                
                # Generate and send trip_start event
                trip_start_event, base_data = self.generator.generate_trip_start_event()
                
                # Send asynchronously
                future = self.sender.send_event_async(trip_start_event)
                
                # Schedule trip_end
                if base_data:
                    trip_duration = random.randint(*trip_duration_range)
                    self.active_trips[trip_start_event['trip_id']] = (
                        trip_start_event, 
                        base_data, 
                        time.time() + trip_duration
                    )
                
                # Check for trips ready to end
                current_time = time.time()
                trips_to_end = []
                
                for trip_id, (start_event, trip_base_data, end_time) in self.active_trips.items():
                    if current_time >= end_time:
                        trips_to_end.append(trip_id)
                
                # Send trip_end events
                for trip_id in trips_to_end:
                    start_event, trip_base_data, _ = self.active_trips.pop(trip_id)
                    trip_end_event = self.generator.generate_trip_end_event(start_event, trip_base_data)
                    self.sender.send_event_async(trip_end_event)
                
                # Print periodic stats
                if time.time() >= next_stats_time:
                    self._print_stats()
                    next_stats_time = time.time() + 30
                
                # Sleep to maintain rate
                elapsed = time.time() - loop_start
                sleep_time = max(0, event_interval - elapsed)
                if sleep_time > 0:
                    time.sleep(sleep_time)
                    
        except KeyboardInterrupt:
            logger.info("🛑 Simulation interrupted by user")
            self.running = False
        
        # Complete remaining trips
        self._complete_remaining_trips()
        
        # Final stats
        self._print_final_stats()
        return True
    
    def _print_stats(self):
        """Print current statistics"""
        system_metrics = self.sender.get_metrics()
        sender_stats = self.sender.get_stats()
        
        logger.info("📊 Current Stats:")
        logger.info(f"   Sender: {sender_stats['sent']} sent, {sender_stats['failed']} failed, {sender_stats['events_per_second']:.1f} eps")
        logger.info(f"   Active trips: {len(self.active_trips)}")
        
        if system_metrics:
            events = system_metrics.get('events', {})
            trips = system_metrics.get('trips', {})
            logger.info(f"   System: {events.get('total_events', 0)} total events, {trips.get('completed_trips', 0)} completed trips")
    
    def _complete_remaining_trips(self):
        """Complete any remaining active trips"""
        if self.active_trips:
            logger.info(f"🏁 Completing {len(self.active_trips)} remaining trips...")
            
            for trip_id, (start_event, base_data, _) in self.active_trips.items():
                trip_end_event = self.generator.generate_trip_end_event(start_event, base_data)
                self.sender.send_event(trip_end_event)  # Synchronous for cleanup
            
            self.active_trips.clear()
    
    def _print_final_stats(self):
        """Print final simulation statistics"""
        logger.info("🏁 Simulation completed")
        
        # Wait a moment for final processing
        time.sleep(2)
        
        final_metrics = self.sender.get_metrics()
        sender_stats = self.sender.get_stats()
        
        logger.info("📈 Final Statistics:")
        logger.info(f"   Sender Performance:")
        logger.info(f"     Events sent: {sender_stats['sent']}")
        logger.info(f"     Events failed: {sender_stats['failed']}")
        logger.info(f"     Average EPS: {sender_stats['events_per_second']:.2f}")
        logger.info(f"     Duration: {sender_stats['elapsed_seconds']:.1f}s")
        
        if final_metrics:
            events = final_metrics.get('events', {})
            trips = final_metrics.get('trips', {})
            logger.info(f"   System Processing:")
            logger.info(f"     Total events: {events.get('total_events', 0)}")
            logger.info(f"     Trip starts: {events.get('trip_starts', 0)}")
            logger.info(f"     Trip ends: {events.get('trip_ends', 0)}")
            logger.info(f"     Completed trips: {trips.get('completed_trips', 0)}")
            logger.info(f"     Active trips: {trips.get('active_trips', 0)}")
            
            latest_metrics = final_metrics.get('latest_daily_metrics')
            if latest_metrics:
                logger.info(f"   Today's Revenue:")
                logger.info(f"     Total fare: ${latest_metrics.get('total_fare', 0):.2f}")
                logger.info(f"     Trip count: {latest_metrics.get('trip_count', 0)}")
                logger.info(f"     Average fare: ${latest_metrics.get('average_fare', 0):.2f}")

def create_sample_csv_files():
    """Create sample CSV files if they don't exist"""
    
    # Create data directory
    os.makedirs('data', exist_ok=True)
    
    # Enhanced sample trip_start.csv
    trip_start_data = [
        ['trip_id', 'pickup_location_id', 'dropoff_location_id', 'vendor_id', 'pickup_datetime', 'estimated_dropoff_datetime', 'estimated_fare_amount'],
        ['c66ce556bc', '93', '93', '1', '2024-05-25 13:19:00', '2024-05-25 14:03:00', '34.18'],
        ['d77df667cd', '45', '78', '2', '2024-05-25 14:22:00', '2024-05-25 15:15:00', '28.50'],
        ['e88ef778de', '112', '156', '1', '2024-05-25 15:30:00', '2024-05-25 16:05:00', '22.75'],
        ['f99fg889ef', '67', '89', '3', '2024-05-25 16:45:00', '2024-05-25 17:20:00', '31.25'],
        ['g00gh990fg', '134', '201', '2', '2024-05-25 18:10:00', '2024-05-25 19:00:00', '45.80'],
        ['h11hi001gh', '88', '167', '1', '2024-05-25 19:30:00', '2024-05-25 20:15:00', '38.90'],
        ['i22ij112hi', '203', '45', '3', '2024-05-25 20:45:00', '2024-05-25 21:30:00', '42.30'],
        ['j33jk223ij', '156', '134', '2', '2024-05-25 21:15:00', '2024-05-25 22:00:00', '29.80']
    ]
    
    # Enhanced sample trip_end.csv  
    trip_end_data = [
        ['dropoff_datetime', 'rate_code', 'passenger_count', 'trip_distance', 'fare_amount', 'tip_amount', 'payment_type', 'trip_type', 'trip_id'],
        ['2024-05-25 14:05:00', '5.0', '1.0', '8.1', '40.09', '4.50', '1.0', '1.0', 'c66ce556bc'],
        ['2024-05-25 15:18:00', '3.0', '2.0', '6.5', '32.25', '3.20', '2.0', '1.0', 'd77df667cd'],
        ['2024-05-25 16:08:00', '2.0', '1.0', '4.2', '25.90', '2.00', '1.0', '2.0', 'e88ef778de'],
        ['2024-05-25 17:25:00', '4.0', '3.0', '7.8', '35.75', '5.50', '1.0', '1.0', 'f99fg889ef'],
        ['2024-05-25 19:05:00', '5.0', '2.0', '12.3', '48.60', '6.80', '3.0', '1.0', 'g00gh990fg'],
        ['2024-05-25 20:20:00', '3.0', '1.0', '9.7', '44.25', '4.00', '2.0', '1.0', 'h11hi001gh'],
        ['2024-05-25 21:35:00', '4.0', '4.0', '11.2', '49.80', '7.20', '1.0', '1.0', 'i22ij112hi'],
        ['2024-05-25 22:10:00', '2.0', '2.0', '6.8', '33.90', '3.50', '2.0', '2.0', 'j33jk223ij']
    ]
    
    # Write trip_start.csv
    trip_start_path = 'data/trip_start.csv'
    if not os.path.exists(trip_start_path):
        with open(trip_start_path, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerows(trip_start_data)
        logger.info(f"Created sample {trip_start_path}")
    
    # Write trip_end.csv
    trip_end_path = 'data/trip_end.csv'
    if not os.path.exists(trip_end_path):
        with open(trip_end_path, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerows(trip_end_data)
        logger.info(f"Created sample {trip_end_path}")

def run_load_test(api_url: str, max_events_per_second: int = 50, duration_minutes: int = 5):
    """Run a comprehensive load test"""
    logger.info(f"🔥 Starting load test: up to {max_events_per_second} EPS for {duration_minutes} minutes")
    
    # Ramp up test - gradually increase load
    ramp_steps = 5
    base_eps = 2
    step_duration = (duration_minutes * 60) // ramp_steps
    
    simulator = TripSimulator('data/trip_start.csv', 'data/trip_end.csv', api_url)
    
    for step in range(ramp_steps):
        current_eps = base_eps + (step * (max_events_per_second - base_eps) // (ramp_steps - 1))
        logger.info(f"📈 Load test step {step + 1}/{ramp_steps}: {current_eps} EPS for {step_duration}s")
        
        simulator.run_simulation(
            events_per_second=current_eps,
            duration_seconds=step_duration,
            trip_duration_range=(30, 180)  # Shorter trips for load testing
        )
        
        if step < ramp_steps - 1:
            logger.info("⏸️ Brief pause between load test steps...")
            time.sleep(5)
    
    logger.info("🏁 Load test completed")

def monitor_system(api_url: str, duration_seconds: int = 60):
    """Monitor system performance"""
    logger.info(f"👁️ Monitoring system for {duration_seconds} seconds...")
    
    sender = EventSender(api_url)
    
    if not sender.check_health():
        logger.error("❌ System not healthy")
        return
    
    start_time = time.time()
    previous_metrics = sender.get_metrics()
    
    while (time.time() - start_time) < duration_seconds:
        time.sleep(10)  # Check every 10 seconds
        
        current_metrics = sender.get_metrics()
        
        if current_metrics and previous_metrics:
            # Calculate deltas
            events_delta = current_metrics['events']['total_events'] - previous_metrics['events']['total_events']
            trips_delta = current_metrics['trips']['completed_trips'] - previous_metrics['trips']['completed_trips']
            
            logger.info(f"📊 Last 10s: +{events_delta} events, +{trips_delta} completed trips")
            logger.info(f"   Active trips: {current_metrics['trips']['active_trips']}")
            
            if current_metrics.get('latest_daily_metrics'):
                metrics = current_metrics['latest_daily_metrics']
                logger.info(f"   Today's total: {metrics['trip_count']} trips, ${metrics['total_fare']:.2f}")
        
        previous_metrics = current_metrics
    
    logger.info("👁️ Monitoring completed")

def main():
    """Main function with comprehensive CLI"""
    parser = argparse.ArgumentParser(description='NSP Bolt Ride Trip Event Simulator')
    
    # File arguments
    parser.add_argument('--trip-start-csv', default='data/trip_start.csv', help='Trip start CSV file')
    parser.add_argument('--trip-end-csv', default='data/trip_end.csv', help='Trip end CSV file')
    parser.add_argument('--api-url', default='http://localhost:5000', help='Processor API URL')
    
    # Simulation parameters
    parser.add_argument('--events-per-second', type=float, default=5.0, help='Events per second')
    parser.add_argument('--duration', type=int, default=300, help='Simulation duration in seconds')
    parser.add_argument('--min-trip-duration', type=int, default=60, help='Minimum trip duration in seconds')
    parser.add_argument('--max-trip-duration', type=int, default=300, help='Maximum trip duration in seconds')
    
    # Actions
    parser.add_argument('--create-samples', action='store_true', help='Create sample CSV files')
    parser.add_argument('--load-test', action='store_true', help='Run load test')
    parser.add_argument('--monitor', action='store_true', help='Monitor system performance')
    parser.add_argument('--quick-test', action='store_true', help='Run quick 60-second test')
    
    # Load test parameters
    parser.add_argument('--max-eps', type=int, default=50, help='Maximum events per second for load test')
    parser.add_argument('--load-duration', type=int, default=5, help='Load test duration in minutes')
    
    args = parser.parse_args()
    
    # Handle actions
    if args.create_samples:
        create_sample_csv_files()
        logger.info("✅ Sample CSV files created. You can now run simulations.")
        return
    
    if args.monitor:
        monitor_system(args.api_url, args.duration)
        return
    
    if args.load_test:
        run_load_test(args.api_url, args.max_eps, args.load_duration)
        return
    
    # Check if CSV files exist
    if not os.path.exists(args.trip_start_csv):
        logger.error(f"Trip start CSV not found: {args.trip_start_csv}")
        logger.info("Run with --create-samples to create sample files")
        return
    
    if not os.path.exists(args.trip_end_csv):
        logger.error(f"Trip end CSV not found: {args.trip_end_csv}")
        logger.info("Run with --create-samples to create sample files")
        return
    
    # Quick test mode
    if args.quick_test:
        args.duration = 60
        args.events_per_second = 10
        logger.info("🚀 Quick test mode: 10 EPS for 60 seconds")
    
    # Run simulation
    simulator = TripSimulator(args.trip_start_csv, args.trip_end_csv, args.api_url)
    success = simulator.run_simulation(
        events_per_second=args.events_per_second,
        duration_seconds=args.duration,
        trip_duration_range=(args.min_trip_duration, args.max_trip_duration)
    )
    
    if success:
        logger.info("🎉 Simulation completed successfully!")
        logger.info("\n📋 Next steps:")
        logger.info("   - Check analytics: ls analytics_output/")
        logger.info("   - View metrics: curl http://localhost:5000/metrics")
        logger.info("   - Run load test: python simulator.py --load-test")
    else:
        logger.error("❌ Simulation failed. Check the logs above.")

if __name__ == '__main__':
    main()