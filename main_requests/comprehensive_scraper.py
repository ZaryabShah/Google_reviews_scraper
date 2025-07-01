import requests
import json
import re
from urllib.parse import unquote
from datetime import datetime
import traceback
import time
import os

class GoogleMapsReviewScraper:
    def __init__(self, place_id):
        self.place_id = place_id.replace("0x", "") if place_id.startswith("0x") else place_id
        self.base_url = "https://www.google.com/maps/rpc/listugcposts"
        self.headers = {
            "accept": "*/*",
            "accept-language": "en-US,en;q=0.9",
            "cache-control": "no-cache",
            "downlink": "1.4",
            "pragma": "no-cache",
            "priority": "u=1, i",
            "referer": "https://www.google.com/",
            "rtt": "150",
            "sec-ch-ua": "\"Not A(Brand\";v=\"99\", \"Google Chrome\";v=\"121\", \"Chromium\";v=\"121\"",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
        }
        self.all_reviews = []
        self.seen_review_ids = set()
        # Get the directory where the script is located
        script_dir = os.path.dirname(os.path.abspath(__file__))
        # Clean place_id for filename (replace colons with underscores)
        clean_place_id = self.place_id.replace(":", "_")
        self.output_file = os.path.join(script_dir, f"reviews_{clean_place_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
        
    def build_querystring(self, continuation_token=None):
        """Build the querystring for the request"""
        if continuation_token:
            pb_value = f"!1m6!1s0x{self.place_id}!6m4!4m1!1e1!4m1!1e3!2m2!1i20!2s{continuation_token}!5m2!1sStliaIi6EPWA9u8PwLTBwAE!7e81!8m9!2b1!3b1!5b1!7b1!12m4!1b1!2b1!4m1!1e1!11m0!13m1!1e1"
        else:
            pb_value = f"!1m6!1s0x{self.place_id}!6m4!4m1!1e1!4m1!1e3!2m2!1i20!2s!5m2!1sStliaIi6EPWA9u8PwLTBwAE!7e81!8m9!2b1!3b1!5b1!7b1!12m4!1b1!2b1!4m1!1e1!11m0!13m1!1e1"
        
        return {
            "authuser": "0",
            "hl": "en",
            "pb": pb_value
        }
    
    def extract_caesy_tokens(self, html_content):
        """Extract all tokens starting with CAESY0"""
        caesy_tokens = re.findall(r'CAESY0[A-Za-z0-9_\-+=]{10,}', html_content)
        
        # Remove duplicates while preserving order
        unique_tokens = []
        seen = set()
        for token in caesy_tokens:
            if token not in seen:
                unique_tokens.append(token)
                seen.add(token)
        
        return unique_tokens

    def parse_timestamp(self, timestamp_microseconds):
        """Convert microsecond timestamp to ISO format"""
        try:
            if timestamp_microseconds:
                timestamp_seconds = int(timestamp_microseconds) / 1000000
                dt = datetime.fromtimestamp(timestamp_seconds)
                return dt.isoformat()
        except:
            pass
        return None

    def extract_place_id_and_coordinates(self, html_content):
        """Extract place ID and coordinates from the response"""
        place_data = {}
        
        # Extract place ID (hex format)
        place_id_match = re.search(r'"0x0:(0x[a-f0-9]+)"', html_content)
        if place_id_match:
            place_data['place_id'] = place_id_match.group(1)
        else:
            place_data['place_id'] = f'0x{self.place_id}'
        
        # Default coordinates
        place_data['latitude'] = 40.0
        place_data['longitude'] = 40.0
        
        return place_data

    def extract_reviewer_names(self, html_content):
        """Extract reviewer names using multiple patterns"""
        names = []
        
        # Pattern 1: Name before profile image URL
        pattern1 = r'"([A-Za-z][^"]{2,49})","https://lh3\.googleusercontent\.com/'
        matches1 = re.findall(pattern1, html_content)
        names.extend(matches1)
        
        # Pattern 2: Name in contributor array
        pattern2 = r',\["([A-Za-z][^"]{2,30})","https://lh3\.googleusercontent\.com/'
        matches2 = re.findall(pattern2, html_content)
        names.extend(matches2)
        
        # Pattern 3: Direct extraction from known structure
        pattern3 = r'"([A-Za-z][^"]{2,30})"\s*,\s*"https://lh3\.googleusercontent\.com/'
        matches3 = re.findall(pattern3, html_content)
        names.extend(matches3)
        
        # Filter out obvious non-names
        filtered_names = []
        excluded_words = ['google', 'maps', 'contrib', 'review', 'local', 'guide', 'http', 'www', 'com', 'net', 'org']
        
        for name in names:
            name_clean = name.strip()
            if (not name_clean.startswith('http') and 
                not name_clean.isdigit() and 
                not any(word in name_clean.lower() for word in excluded_words) and
                len(name_clean.split()) <= 4 and
                not re.match(r'^[A-Z0-9_\-+=]+$', name_clean)):
                filtered_names.append(name_clean)
        
        # Remove duplicates while preserving order
        unique_names = []
        seen = set()
        for name in filtered_names:
            if name.lower() not in seen:
                unique_names.append(name)
                seen.add(name.lower())
        
        return unique_names

    def extract_review_texts(self, html_content):
        """Extract review texts using multiple patterns"""
        texts = []
        
        # Pattern 1: Text in specific JSON structure
        pattern1 = r',\["([^"]{20,500})"\s*,\s*null\s*,\s*\[\d+,\d+\]\]'
        matches1 = re.findall(pattern1, html_content)
        texts.extend(matches1)
        
        # Pattern 2: Alternative structure
        pattern2 = r'"([^"]{30,500})",null,\[\d+,\d+\]'
        matches2 = re.findall(pattern2, html_content)
        texts.extend(matches2)
        
        # Pattern 3: Simple text extraction
        pattern3 = r'"([^"]{40,400})"'
        potential_texts = re.findall(pattern3, html_content)
        
        # Filter potential texts for actual review content
        for text in potential_texts:
            if (not text.startswith('http') and 
                not text.startswith('Ch') and
                not text.startswith('0ah') and
                not text.startswith('CAESY') and
                ' ' in text and
                any(word in text.lower() for word in ['food', 'good', 'great', 'bad', 'excellent', 'love', 'like', 'ordered', 'ate', 'meal', 'restaurant', 'place', 'service', 'staff', 'time', 'experience'])):
                texts.append(text)
        
        # Clean and filter texts
        filtered_texts = []
        for text in texts:
            clean_text = text.replace('\\n', '\n').replace('\\"', '"').replace('\\/', '/')
            
            if (not clean_text.startswith('http') and 
                not clean_text.startswith('Ch') and
                not clean_text.startswith('0ah') and
                len(clean_text.strip()) > 15 and
                not re.match(r'^[A-Z0-9_\-+=]+$', clean_text)):
                filtered_texts.append(clean_text.strip())
        
        # Remove duplicates while preserving order
        unique_texts = []
        seen = set()
        for text in filtered_texts:
            if text.lower() not in seen:
                unique_texts.append(text)
                seen.add(text.lower())
        
        return unique_texts

    def extract_star_ratings(self, html_content):
        """Extract star ratings from the HTML"""
        ratings = []
        
        # Pattern 1: Direct rating in arrays
        pattern1 = r'\[\[([1-5])\]'
        matches1 = re.findall(pattern1, html_content)
        ratings.extend([int(m) for m in matches1])
        
        # Pattern 2: Rating in nested structure
        pattern2 = r'"stars":\s*([1-5])'
        matches2 = re.findall(pattern2, html_content)
        ratings.extend([int(m) for m in matches2])
        
        return ratings

    def extract_time_ago_strings(self, html_content):
        """Extract 'time ago' strings from the HTML"""
        time_patterns = [
            r'"((?:\d+\s+)?(?:year|month|week|day|hour|minute)s?\s+ago)"',
            r'"(Edited\s+(?:\d+\s+)?(?:year|month|week|day|hour|minute)s?\s+ago)"',
            r'"(a\s+(?:year|month|week|day|hour|minute)\s+ago)"'
        ]
        
        time_strings = []
        for pattern in time_patterns:
            matches = re.findall(pattern, html_content, re.IGNORECASE)
            time_strings.extend(matches)
        
        return time_strings

    def parse_reviews_from_response(self, html_content):
        """Parse reviews from the HTML response"""
        reviews = []
        place_data = self.extract_place_id_and_coordinates(html_content)
        
        try:
            print("Extracting reviews data...")
            
            # Extract all components
            review_ids = re.findall(r'"(Ch[ZdDSUH][A-Za-z0-9]{20,})"', html_content)
            reviewer_ids = re.findall(r'"(\d{21})"', html_content)
            profile_images = re.findall(r'"(https://lh3\.googleusercontent\.com/[^"]+)"', html_content)
            timestamps = re.findall(r'(\d{13,})', html_content)
            
            # Dynamic extraction
            reviewer_names = self.extract_reviewer_names(html_content)
            review_texts = self.extract_review_texts(html_content)
            star_ratings = self.extract_star_ratings(html_content)
            time_agos = self.extract_time_ago_strings(html_content)
            
            print(f"Found: {len(reviewer_names)} names, {len(review_texts)} texts, {len(star_ratings)} ratings")
            
            # Build reviews
            max_reviews = min(len(review_ids), 20)
            
            for i in range(max_reviews):
                review_id = review_ids[i] if i < len(review_ids) else f"review_{i}_{int(time.time())}"
                
                # Skip if we've already seen this review
                if review_id in self.seen_review_ids:
                    # print(f"Skipping duplicate review: {review_id}")
                    continue
                
                # Get timestamps
                published_timestamp = timestamps[i*2] if i*2 < len(timestamps) else None
                last_edited_timestamp = timestamps[i*2+1] if i*2+1 < len(timestamps) else published_timestamp
                
                review = {
                    "reviewerId": reviewer_ids[i] if i < len(reviewer_ids) else f"reviewer_{i}",
                    "reviewerUrl": f"https://www.google.com/maps/contrib/{reviewer_ids[i]}?hl=en" if i < len(reviewer_ids) else "",
                    "reviewerName": reviewer_names[i] if i < len(reviewer_names) else f"Reviewer {i+1}",
                    "reviewerNumberOfReviews": 0,
                    "reviewerPhotoUrl": profile_images[i] if i < len(profile_images) else "",
                    "text": review_texts[i] if i < len(review_texts) else "",
                    "reviewImageUrls": [],
                    "publishedAtDate": self.parse_timestamp(published_timestamp) if published_timestamp else datetime.now().isoformat(),
                    "lastEditedAtDate": self.parse_timestamp(last_edited_timestamp) if last_edited_timestamp else None,
                    "likesCount": 0,
                    "reviewId": review_id,
                    "reviewUrl": f"https://www.google.com/maps/reviews/data=!4m8!14m7!1m6!2m5!1s{review_id}" if review_id.startswith('Ch') else "",
                    "stars": star_ratings[i] if i < len(star_ratings) else 5,
                    "placeId": place_data.get('place_id', f'0x{self.place_id}'),
                    "location": {
                        "lat": place_data.get('latitude', 40.0),
                        "lng": place_data.get('longitude', 40.0)
                    },
                    "address": "",
                    "neighborhood": "",
                    "street": "",
                    "city": "",
                    "postalCode": "",
                    "categories": [],
                    "title": "",
                    "totalScore": 0.0,
                    "url": "",
                    "price": None,
                    "cid": place_data.get('place_id', ''),
                    "fid": "",
                    "scrapedAt": datetime.now().isoformat(),
                    "timeAgo": time_agos[i] if i < len(time_agos) else ""
                }
                
                reviews.append(review)
                self.seen_review_ids.add(review_id)
                
        except Exception as e:
            print(f"Error parsing reviews: {e}")
            traceback.print_exc()
        
        return reviews

    def make_request(self, continuation_token=None):
        """Make a request to Google Maps API"""
        querystring = self.build_querystring(continuation_token)
        
        try:
            print(f"Making request with token: {continuation_token if continuation_token else 'None (first request)'}")
            response = requests.get(self.base_url, headers=self.headers, params=querystring)
            
            if response.status_code == 200:
                return response.text
            else:
                print(f"Request failed with status code: {response.status_code}")
                return None
                
        except Exception as e:
            print(f"Error making request: {e}")
            return None

    def save_reviews_to_file(self):
        """Save all collected reviews to JSON file"""
        data = {
            'place_id': f'0x{self.place_id}',
            'extraction_timestamp': datetime.now().isoformat(),
            'total_reviews': len(self.all_reviews),
            'reviews': self.all_reviews
        }
        
        try:
            with open(self.output_file, 'w', encoding='utf-8') as file:
                json.dump(data, file, indent=2, ensure_ascii=False)
            print(f"✅ Reviews saved to: {self.output_file}")
        except Exception as e:
            print(f"Error saving reviews: {e}")

    def scrape_all_reviews(self):
        """Main method to scrape all reviews with pagination"""
        print(f"Starting to scrape reviews for place ID: 0x{self.place_id}")
        
        continuation_token = None
        page_number = 1
        
        while True:
            print(f"\n--- Page {page_number} ---")
            
            # Make request
            response_content = self.make_request(continuation_token)
            if not response_content:
                print("Failed to get response, stopping...")
                break
            
            # Parse reviews from response
            new_reviews = self.parse_reviews_from_response(response_content)
            
            if not new_reviews:
                print("No new reviews found, stopping...")
                break
            
            # Add new reviews to collection
            self.all_reviews.extend(new_reviews)
            print(f"Added {len(new_reviews)} new reviews. Total so far: {len(self.all_reviews)}")
            
            # Extract continuation token for next request
            caesy_tokens = self.extract_caesy_tokens(response_content)
            if caesy_tokens:
                continuation_token = caesy_tokens[0]  # Use first token for next request
                print(f"Found continuation token: {continuation_token}")
            else:
                print("No continuation token found, stopping...")
                break
            
            page_number += 1
            
            # Add delay between requests to be respectful
            time.sleep(2)
        
        # Save all reviews to file
        self.save_reviews_to_file()
        
        print(f"\n=== SCRAPING COMPLETE ===")
        print(f"Total reviews scraped: {len(self.all_reviews)}")
        print(f"Total pages processed: {page_number}")
        print(f"Output file: {self.output_file}")

def main():
    # Get place ID from user input
    place_id = input("Enter the place ID (e.g., 89c3ca9c11f90c25:0x6cc8dba851799f09): ").strip()
    
    # Clean the place ID
    if place_id.startswith("1s0x"):
        place_id = place_id[4:]  # Remove "1s0x" prefix
    elif place_id.startswith("0x"):
        place_id = place_id[2:]  # Remove "0x" prefix
    
    # Create scraper instance and start scraping
    scraper = GoogleMapsReviewScraper(place_id)
    scraper.scrape_all_reviews()

if __name__ == "__main__":
    main()
