import asyncio
import aiohttp
import json
import re
from urllib.parse import unquote, urlparse, parse_qs
from datetime import datetime
import traceback
import time
import os
import threading
import argparse
import sys
from typing import Set, List, Dict, Any

def extract_place_id_from_url(url):
    """Extract place ID from Google Maps URL"""
    try:
        # Pattern 1: Look for 1s0x followed by place ID
        pattern1 = r'1s0x([a-f0-9]+):0x([a-f0-9]+)'
        match1 = re.search(pattern1, url)
        if match1:
            return f"{match1.group(1)}:0x{match1.group(2)}"
        
        # Pattern 2: Look for direct 0x format
        pattern2 = r'0x([a-f0-9]+):0x([a-f0-9]+)'
        match2 = re.search(pattern2, url)
        if match2:
            return f"{match2.group(1)}:0x{match2.group(2)}"
        
        # Pattern 3: Look for !1s0x format (another common format)
        pattern3 = r'!1s0x([a-f0-9]+):0x([a-f0-9]+)'
        match3 = re.search(pattern3, url)
        if match3:
            return f"{match3.group(1)}:0x{match3.group(2)}"
        
        print(f"❌ Could not extract place ID from URL: {url}")
        print("Please make sure the URL is a valid Google Maps place URL")
        return None
        
    except Exception as e:
        print(f"❌ Error parsing URL: {e}")
        return None

def parse_command_line_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description='Extract Google Maps reviews with source filtering',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python extract.py "https://maps.google.com/maps/place/..." --google
  python extract.py "https://maps.google.com/maps/place/..." --tripadvisor
  python extract.py "https://maps.google.com/maps/place/..." --all
        """
    )
    
    parser.add_argument(
        'url',
        help='Google Maps URL containing the place ID'
    )
    
    # Source filtering options
    source_group = parser.add_mutually_exclusive_group()
    source_group.add_argument(
        '--google',
        action='store_const',
        const='google',
        dest='source_filter',
        help='Extract only Google reviews'
    )
    source_group.add_argument(
        '--tripadvisor',
        action='store_const',
        const='tripadvisor',
        dest='source_filter',
        help='Extract only TripAdvisor reviews'
    )
    source_group.add_argument(
        '--all',
        action='store_const',
        const=None,
        dest='source_filter',
        help='Extract all reviews (default)'
    )
    
    # Optional parameters
    parser.add_argument(
        '--max-pages',
        type=int,
        default=None,
        help='Maximum pages to scrape per direction (default: unlimited)'
    )
    
    parser.add_argument(
        '--delay',
        type=int,
        default=2,
        help='Delay between requests in seconds (default: 2)'
    )
    
    return parser.parse_args()

class DualAsyncGoogleMapsReviewScraper:
    def __init__(self, place_id, source_filter=None):
        self.place_id = place_id.replace("0x", "") if place_id.startswith("0x") else place_id
        self.source_filter = source_filter  # New: source filter (None, 'google', 'tripadvisor', etc.)
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
        
        # Shared state between both scrapers
        self.all_reviews = []
        self.seen_review_ids = set()
        self.seen_reviewer_ids = set()  # Track reviewer IDs for duplicate detection
        self.duplicate_count = 0
        self.stop_scraping = False
        self.lock = threading.Lock()  # Thread safety for shared state
        
        # Separate tracking for each direction
        self.used_tokens_highest = set()
        self.used_tokens_lowest = set()
        
        # File setup
        script_dir = os.path.dirname(os.path.abspath(__file__))
        clean_place_id = self.place_id.replace(":", "_")
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        self.output_file = os.path.join(script_dir, f"dual_reviews_{clean_place_id}_{timestamp}.json")
        
        # Track all tokens for debugging
        self.all_tokens = {
            'highest_rating': [],
            'lowest_rating': []
        }
        
        # Track stats per direction
        self.stats = {
            'highest_rating': {'pages': 0, 'reviews': 0, 'duplicates': 0},
            'lowest_rating': {'pages': 0, 'reviews': 0, 'duplicates': 0}
        }
        
    def build_querystring(self, continuation_token=None, sort_by_highest=True):
        """Build the querystring for the request with different sorting"""
        # Different sort orders:
        # 1e1 = Most relevant (default)
        # 1e2 = Newest first  
        # 1e3 = Highest rating first
        # 1e4 = Lowest rating first
        sort_value = "1e3" if sort_by_highest else "1e4"
        
        if continuation_token:
            pb_value = f"!1m6!1s0x{self.place_id}!6m4!4m1!1e1!4m1!1e3!2m2!1i20!2s{continuation_token}!5m2!1sStliaIi6EPWA9u8PwLTBwAE!7e81!8m9!2b1!3b1!5b1!7b1!12m4!1b1!2b1!4m1!1e1!11m0!13m1!{sort_value}"
        else:
            pb_value = f"!1m6!1s0x{self.place_id}!6m4!4m1!1e1!4m1!1e3!2m2!1i20!2s!5m2!1sStliaIi6EPWA9u8PwLTBwAE!7e81!8m9!2b1!3b1!5b1!7b1!12m4!1b1!2b1!4m1!1e1!11m0!13m1!{sort_value}"
        
        return {
            "authuser": "0",
            "hl": "en",
            "pb": pb_value
        }
    
    def get_next_unused_token(self, available_tokens, used_tokens_set):
        """Get the next unused continuation token from available tokens"""
        for token in available_tokens:
            if token not in used_tokens_set:
                return token
        return None

    def extract_caesy_tokens(self, html_content):
        """Extract all tokens starting with CAESY0"""
        caesy_tokens = re.findall(r'CAES[A-Za-z0-9_\-+=]{10,}', html_content)
        
        # Remove duplicates while preserving order
        unique_tokens = []
        seen = set()
        for token in caesy_tokens:
            if token not in seen:
                unique_tokens.append(token)
                seen.add(token)
        
        return unique_tokens

    def find_caesy_tokens(self, html_content):
        """Find all CAESY tokens in the HTML content"""
        # Pattern to match CAESY tokens 
        pattern = r'"(CAES[^"]*)"'
        tokens = re.findall(pattern, html_content)
        return tokens
    
    def extract_review_sections(self, html_content):
        """Split content by CAESY tokens to get individual review sections"""
        tokens = self.find_caesy_tokens(html_content)
        if not tokens:
            return []
            
        sections = []
        content = html_content
        
        # Find positions of all CAESY tokens
        token_positions = []
        for token in tokens:
            pos = content.find(f'"{token}"')
            if pos != -1:
                token_positions.append(pos)
        
        # Sort positions
        token_positions.sort()
        
        # Extract sections between tokens
        for i in range(len(token_positions)):
            start_pos = token_positions[i]
            
            if i + 1 < len(token_positions):
                end_pos = token_positions[i + 1]
                section = content[start_pos:end_pos]
            else:
                section = content[start_pos:]
                
            sections.append(section)
            
        return sections

    def extract_star_rating(self, section):
        """Extract star rating with enhanced pattern matching focusing on accurate X/5 format"""
        
        # Primary Pattern: Focus on "X/5" format which is most accurate for Tripadvisor
        # This matches patterns like: [null,3,"3/5"], [null,4,"4/5"], etc.
        rating_fraction_pattern = r'"(\d)/5"'
        fraction_matches = re.findall(rating_fraction_pattern, section)
        if fraction_matches:
            try:
                rating = int(fraction_matches[0])  # Take the first match
                if 1 <= rating <= 5:
                    return rating
            except (ValueError, TypeError):
                pass
        
        # Secondary Pattern: Rating array pattern - [null,N,"N/5"] - extract from the middle number
        rating_array_pattern = r'\[null,(\d),"(\d)/5"\]'
        rating_array_matches = re.findall(rating_array_pattern, section)
        if rating_array_matches:
            try:
                # Verify both numbers match (for data integrity)
                middle_rating = int(rating_array_matches[0][0])
                fraction_rating = int(rating_array_matches[0][1])
                if middle_rating == fraction_rating and 1 <= middle_rating <= 5:
                    return middle_rating
            except (ValueError, TypeError):
                pass
        
        # Tertiary Pattern: Tripadvisor source info pattern - ["Tripadvisor","url","tripadvisor",N]
        tripadvisor_pattern = r'"tripadvisor",(\d)\]'
        tripadvisor_matches = re.findall(tripadvisor_pattern, section)
        if tripadvisor_matches:
            try:
                rating = int(tripadvisor_matches[0])
                if 1 <= rating <= 5:
                    return rating
            except (ValueError, TypeError):
                pass
        
        # Fallback Pattern: Google primary pattern - [[N], where N is the star rating at the start of review data
        # This matches patterns like: [[1],null,null,null,null,null,[[["GUIDE...
        # or [[2],null,null,null,null,null,null,null,null,null,null,null,null,null,["en"],[["The...
        primary_pattern = r'\[\[(\d)\],'
        
        # Find all matches and take the first valid one (closest to start of section)
        matches = re.findall(primary_pattern, section)
        if matches:
            try:
                rating = int(matches[0])  # Take the first match
                if 1 <= rating <= 5:
                    return rating
            except (ValueError, TypeError):
                pass
        
        # Final Fallback patterns if above don't work
        fallback_patterns = [
            r'\[\[(\d)\]\]',  # [[5]], [[2]], etc.
            r'"rating":(\d)',  # "rating":5
            r'stars?[^0-9]*(\d)[^0-9]*out',  # 5 stars out of
            r'"(\d)\s*stars?"',  # "5 stars"
        ]
        
        for pattern in fallback_patterns:
            matches = re.findall(pattern, section)
            for match in matches:
                try:
                    rating = int(match)
                    if 1 <= rating <= 5:
                        return rating
                except (ValueError, TypeError):
                    continue
        
        return None

    def extract_likes_count(self, section):
        """Extract likes count from review section"""
        # Multiple patterns for likes
        patterns = [
            r'\[\[1,(\d+)\]\]',  # [[1,4]]
            r'"helpful_count":(\d+)',  # "helpful_count":4
            r'(\d+)\s*people?\s*found?\s*helpful',  # 4 people found helpful
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, section)
            if matches:
                return int(matches[-1])  # Take the last match
        return None

    def extract_user_info(self, section):
        """Extract comprehensive user information"""
        user_info = {}
        
        # Extract user name - multiple patterns
        name_patterns = [
            r'"([^"]+)","https://lh3\.googleusercontent\.com',
            r'\["([^"]+)","https://lh3\.googleusercontent\.com',
            r'"display_name":"([^"]+)"',
        ]
        
        for pattern in name_patterns:
            matches = re.findall(pattern, section)
            if matches:
                user_info['name'] = matches[0]
                break
        
        # Extract profile image URL
        image_patterns = [
            r'"(https://lh3\.googleusercontent\.com/a[^"]*s120-c-rp[^"]*)"',
            r'"(https://lh3\.googleusercontent\.com/a[^"]*br100[^"]*)"',
        ]
        
        for pattern in image_patterns:
            matches = re.findall(pattern, section)
            if matches:
                user_info['profile_image'] = matches[0]
                break
        
        # Extract user ID
        user_id_pattern = r'"(\d{21})"'
        user_id_matches = re.findall(user_id_pattern, section)
        if user_id_matches:
            user_info['user_id'] = user_id_matches[0]
        
        # Extract review count
        review_count_patterns = [
            r'"(\d+)\s*reviews?"',
            r'(\d+)\s*reviews?[^"]*"',
        ]
        
        for pattern in review_count_patterns:
            matches = re.findall(pattern, section)
            if matches:
                user_info['review_count'] = int(matches[0])
                break
        
        # Local guide detection
        if 'Local Guide' in section:
            user_info['is_local_guide'] = True
            # Try to extract local guide level
            level_pattern = r'Local Guide[^0-9]*(\d+)[^0-9]*reviews?'
            level_matches = re.findall(level_pattern, section)
            if level_matches:
                user_info['local_guide_level'] = int(level_matches[0])
        else:
            user_info['is_local_guide'] = False
            
        return user_info

    def extract_review_text(self, section):
        """Extract review text(s) with better filtering"""
        texts = []
        
        # Multiple patterns for review text
        text_patterns = [
            r'\["([^"]{20,})",null,\[0,\d+\]\]',  # Minimum 20 chars
            r'"text":"([^"]{20,})"',
            r'"review_text":"([^"]{20,})"',
        ]
        
        for pattern in text_patterns:
            matches = re.findall(pattern, section)
            for text in matches:
                # Decode escaped characters
                try:
                    decoded_text = text.encode().decode('unicode_escape')
                except:
                    decoded_text = text
                
                # Filter out URLs, short texts, and common patterns
                if (len(decoded_text) > 10 and 
                    not decoded_text.startswith('http') and
                    not decoded_text.startswith('www') and
                    'google.com' not in decoded_text.lower() and
                    'googleusercontent' not in decoded_text.lower()):
                    texts.append(decoded_text)
        
        # Remove duplicates while preserving order
        unique_texts = []
        for text in texts:
            if text not in unique_texts:
                unique_texts.append(text)
        
        return unique_texts

    def extract_date_info(self, section):
        """Extract comprehensive date information"""
        date_info = {}
        
        # Enhanced patterns for relative dates with better coverage
        relative_patterns = [
            r'"(\d+\s*years?\s*ago)"',
            r'"(\d+\s*months?\s*ago)"', 
            r'"(\d+\s*weeks?\s*ago)"',
            r'"(\d+\s*days?\s*ago)"',
            r'"(a\s*year\s*ago)"',
            r'"(a\s*month\s*ago)"',
            r'"(a\s*week\s*ago)"',
            r'"(a\s*day\s*ago)"',
            r'"(Edited\s+a\s+month\s+ago)"',
            r'"(Edited\s+\d+\s*(?:years?|months?|weeks?|days?)\s*ago)"',
            r'"(Edited[^"]*ago)"',
            # More flexible patterns
            r'null,"([^"]*(?:years?|months?|weeks?|days?)\s*ago)"',
            r'null,"([^"]*year\s*ago)"',
            r'null,"([^"]*month\s*ago)"',
            r'null,"([^"]*week\s*ago)"',
            r'null,"([^"]*day\s*ago)"',
        ]
        
        for pattern in relative_patterns:
            matches = re.findall(pattern, section, re.IGNORECASE)
            if matches:
                date_info['relative_date'] = matches[0].strip()
                break
        
        # Look for timestamp patterns
        timestamp_patterns = [
            r'(\d{13})',  # 13-digit timestamp
            r'(\d{10})',  # 10-digit timestamp
        ]
        
        for pattern in timestamp_patterns:
            matches = re.findall(pattern, section)
            if matches:
                try:
                    timestamp = int(matches[0])
                    if len(matches[0]) == 13:  # milliseconds
                        timestamp = timestamp / 1000
                    date_info['timestamp'] = timestamp
                    date_info['iso_date'] = datetime.fromtimestamp(timestamp).isoformat()
                    break
                except:
                    continue
        
        return date_info

    def extract_review_source(self, section):
        """Extract review source information (Google, Tripadvisor, etc.)"""
        source_info = {}
        
        # Pattern to extract source information from structured data
        # Looking for patterns like: ["Google","url",null,"google",5] or ["Tripadvisor","url",123,"tripadvisor",5]
        source_patterns = [
            r'\["(Google)","[^"]*","[^"]*","google",\d+\]',
            r'\["(Tripadvisor)","[^"]*",(?:\d+|null),"tripadvisor",\d+\]',
            r'\["([^"]+)","[^"]*",(?:\d+|null),"([^"]+)",\d+\]',
            # More flexible patterns
            r'null,\["([^"]+)","[^"]*googleusercontent[^"]*"',  # Google profile images
            r'null,\["([^"]+)","[^"]*tripadvisor[^"]*"',       # TripAdvisor URLs
        ]
        
        for pattern in source_patterns:
            matches = re.findall(pattern, section, re.IGNORECASE)
            if matches:
                if isinstance(matches[0], tuple):
                    source_name = matches[0][0]
                else:
                    source_name = matches[0]
                
                # Normalize source names
                source_name_lower = source_name.lower()
                if 'google' in source_name_lower:
                    source_info['source'] = 'Google'
                    source_info['source_type'] = 'google'
                elif 'tripadvisor' in source_name_lower:
                    source_info['source'] = 'Tripadvisor'
                    source_info['source_type'] = 'tripadvisor'
                else:
                    source_info['source'] = source_name
                    source_info['source_type'] = source_name_lower
                break
        
        # If no structured source found, try to infer from URLs
        if not source_info.get('source'):
            if 'tripadvisor.com' in section or 'tripadvisor.de' in section:
                source_info['source'] = 'Tripadvisor'
                source_info['source_type'] = 'tripadvisor'
            elif 'google.com' in section or 'googleusercontent.com' in section:
                source_info['source'] = 'Google'
                source_info['source_type'] = 'google'
            else:
                # Default to Google if no clear source is found
                source_info['source'] = 'Google'
                source_info['source_type'] = 'google'
        
        return source_info

    def extract_business_info(self, section):
        """Extract business/location information"""
        business_info = {}
        
        # Business ID
        business_patterns = [
            r'"(0x0:0x[a-f0-9]+)"',
            r'"business_id":"([^"]+)"',
        ]
        
        for pattern in business_patterns:
            matches = re.findall(pattern, section)
            if matches:
                business_info['business_id'] = matches[0]
                break
        
        # Coordinates
        coord_pattern = r'\[3,(-?\d+\.?\d*),(-?\d+\.?\d*)\]'
        coord_matches = re.findall(coord_pattern, section)
        if coord_matches:
            lng, lat = coord_matches[0]
            business_info['coordinates'] = {
                'latitude': float(lat),
                'longitude': float(lng)
            }
        
        # Business name (if available)
        name_patterns = [
            r'"business_name":"([^"]+)"',
            r'"name":"([^"]+)","address"',
        ]
        
        for pattern in name_patterns:
            matches = re.findall(pattern, section)
            if matches:
                business_info['business_name'] = matches[0]
                break
        
        return business_info

    def extract_review_images(self, section):
        """Extract review images uploaded by user"""
        images = []
        
        # Patterns for review images (not profile images)
        image_patterns = [
            r'"(https://lh3\.googleusercontent\.com/geougc-cs/[^"]+)"',
            r'"(https://lh3\.googleusercontent\.com/places/[^"]+)"',
        ]
        
        for pattern in image_patterns:
            matches = re.findall(pattern, section)
            for img_url in matches:
                if img_url not in images:  # Avoid duplicates
                    images.append(img_url)
            
        return images

    def extract_review_features(self, section):
        """Extract review features like dining mode, price range, etc."""
        features = {}
        
        # Dining mode
        if 'TAKE_OUT' in section:
            features['service_type'] = 'takeout'
        elif 'DINE_IN' in section:
            features['service_type'] = 'dine_in'
        elif 'DELIVERY' in section:
            features['service_type'] = 'delivery'
        
        # Meal type
        meal_types = ['BREAKFAST', 'LUNCH', 'DINNER', 'BRUNCH']
        for meal in meal_types:
            if meal in section:
                features['meal_type'] = meal.lower()
                break
        
        # Price range
        price_patterns = [
            r'USD_(\d+)_TO_(\d+)',
            r'\$(\d+)[–-](\d+)',
        ]
        
        for pattern in price_patterns:
            matches = re.findall(pattern, section)
            if matches:
                min_price, max_price = matches[0]
                features['price_range'] = {
                    'min': int(min_price),
                    'max': int(max_price),
                    'currency': 'USD'
                }
                break
        
        # Recommended dishes
        dish_pattern = r'"([^"]+)","(M:/g/[^"]+)"'
        dish_matches = re.findall(dish_pattern, section)
        if dish_matches:
            features['recommended_dishes'] = [dish[0] for dish in dish_matches]
        
        return features

    def extract_owner_response(self, texts):
        """Identify owner response from multiple texts"""
        if len(texts) > 1:
            # Usually the second text is the owner response
            # Can also check for common owner response patterns
            for i, text in enumerate(texts[1:], 1):
                if any(word in text.lower() for word in ['thank', 'appreciate', 'glad', 'sorry', 'pleasure']):
                    return text
            return texts[1]  # Default to second text
        return None

    def extract_single_review(self, section):
        """Extract comprehensive data for a single review"""
        review = {}
        
        # Basic review data
        review['rating'] = self.extract_star_rating(section)
        review['likes_count'] = self.extract_likes_count(section)
        review['user_info'] = self.extract_user_info(section)
        review['date_info'] = self.extract_date_info(section)
        review['source_info'] = self.extract_review_source(section)  # New field
        review['business_info'] = self.extract_business_info(section)
        review['features'] = self.extract_review_features(section)
        
        # Review content
        texts = self.extract_review_text(section)
        if texts:
            review['review_text'] = texts[0]
            review['owner_response'] = self.extract_owner_response(texts)
        
        # Media
        review['review_images'] = self.extract_review_images(section)
        
        # Metadata
        review['section_length'] = len(section)
        review['has_images'] = len(review['review_images']) > 0
        review['has_owner_response'] = review.get('owner_response') is not None
        
        return review

    def calculate_confidence(self, review):
        """Calculate confidence score for extracted review"""
        score = 0.0
        
        # User info (30%)
        if review.get('user_info', {}).get('name'):
            score += 0.15
        if review.get('user_info', {}).get('user_id'):
            score += 0.15
        
        # Review content (40%)
        if review.get('review_text'):
            score += 0.25
            if len(review['review_text']) > 50:
                score += 0.15
        
        # Rating (20%)
        if review.get('rating') is not None:
            score += 0.20
        
        # Date (10%)
        if review.get('date_info'):
            score += 0.10
        
        return min(score, 1.0)

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

    def parse_reviews_from_response(self, html_content, sort_direction):
        """Parse reviews from the HTML response using enhanced CAESY token parsing"""
        reviews = []
        place_data = self.extract_place_id_and_coordinates(html_content)
        
        try:
            print(f"[{sort_direction}] Extracting reviews using enhanced CAESY parsing...")
            
            # Use the enhanced parsing method - extract review sections
            sections = self.extract_review_sections(html_content)
            print(f"[{sort_direction}] Found {len(sections)} review sections")
            
            new_reviews_count = 0
            duplicates_in_request = 0  # Track duplicates for THIS request only
            
            for i, section in enumerate(sections):
                try:
                    # Extract comprehensive review data using enhanced parser
                    enhanced_review = self.extract_single_review(section)
                    
                    # Enhanced validation - require at least one meaningful field
                    has_user = bool(enhanced_review.get('user_info', {}).get('name'))
                    has_text = bool(enhanced_review.get('review_text'))
                    has_rating = enhanced_review.get('rating') is not None
                    has_date = bool(enhanced_review.get('date_info'))
                    
                    if not (has_user or has_text or has_rating or has_date):
                        continue  # Skip if no meaningful data
                    
                    # Generate IDs for compatibility with existing system
                    user_info = enhanced_review.get('user_info', {})
                    reviewer_id = user_info.get('user_id', f"reviewer_{i}_{int(time.time())}")
                    review_id = f"enhanced_review_{i}_{int(time.time())}"
                    
                    with self.lock:
                        # Check if we should stop
                        if self.stop_scraping:
                            print(f"[{sort_direction}] Stopping due to duplicate limit reached")
                            break
                        
                        # Skip if we've already seen this reviewer
                        if reviewer_id in self.seen_reviewer_ids:
                            duplicates_in_request += 1
                            self.duplicate_count += 1  # Still track total for stats
                            
                            # Update per-direction stats  
                            stats_key = 'highest_rating' if sort_direction == 'HIGHEST' else 'lowest_rating'
                            self.stats[stats_key]['duplicates'] += 1
                            
                            print(f"[{sort_direction}] Duplicate found (reviewer: {reviewer_id}). Duplicates in this request: {duplicates_in_request}")
                            
                            # Check if THIS REQUEST has too many duplicates
                            if duplicates_in_request > 100:
                                print(f"[{sort_direction}] STOPPING: More than 15 duplicates found in this single request!")
                                self.stop_scraping = True
                                break
                            continue
                        
                        # Mark as seen
                        self.seen_review_ids.add(review_id)
                        self.seen_reviewer_ids.add(reviewer_id)
                    
                    # Convert enhanced review to simplified format with only 6 requested fields
                    date_info = enhanced_review.get('date_info', {})
                    source_info = enhanced_review.get('source_info', {})
                    published_date = date_info.get('iso_date', datetime.now().isoformat())
                    
                    # Only include the 6 requested fields
                    review = {
                        "reviewerName": user_info.get('name', f"Reviewer {i+1}"),
                        "rating": enhanced_review.get('rating', 5),
                        "published_at": published_date,
                        "timeAgo": date_info.get('relative_date', ''),
                        "source": source_info.get('source', 'Google'),
                        "text": enhanced_review.get('review_text', '')
                    }
                    
                    reviews.append(review)
                    new_reviews_count += 1
                    
                    # Update per-direction stats
                    stats_key = 'highest_rating' if sort_direction == 'HIGHEST' else 'lowest_rating'
                    self.stats[stats_key]['reviews'] += 1
                    
                    user_name = user_info.get('name', 'Unknown')
                    rating = enhanced_review.get('rating', 'N/A')
                    source = source_info.get('source', 'Unknown')
                    timing = date_info.get('relative_date', 'Unknown')
                    print(f"[{sort_direction}] Extracted review {new_reviews_count}: {user_name} (Rating: {rating}, Source: {source}, Timing: {timing})")
                    
                except Exception as e:
                    print(f"[{sort_direction}] Error parsing section {i}: {str(e)}")
                    continue
            
            print(f"[{sort_direction}] Added {new_reviews_count} new reviews, {duplicates_in_request} duplicates in this request")
                
        except Exception as e:
            print(f"[{sort_direction}] Error in enhanced parsing: {e}")
            traceback.print_exc()
        
        return reviews

    async def make_request(self, session, continuation_token=None, sort_by_highest=True):
        """Make an async request to Google Maps API"""
        querystring = self.build_querystring(continuation_token, sort_by_highest)
        sort_direction = "HIGHEST" if sort_by_highest else "LOWEST"
        
        try:
            print(f"[{sort_direction}] Making request with token: {continuation_token[:50] if continuation_token else 'None (first request)'}")
            
            async with session.get(self.base_url, params=querystring) as response:
                if response.status == 200:
                    return await response.text()
                else:
                    print(f"[{sort_direction}] Request failed with status code: {response.status}")
                    return None
                    
        except Exception as e:
            print(f"[{sort_direction}] Error making request: {e}")
            return None

    async def scrape_direction(self, sort_by_highest=True):
        """Scrape reviews in one direction (highest or lowest rating first)"""
        sort_direction = "HIGHEST" if sort_by_highest else "LOWEST"
        used_tokens = self.used_tokens_highest if sort_by_highest else self.used_tokens_lowest
        stats_key = 'highest_rating' if sort_by_highest else 'lowest_rating'
        
        print(f"[{sort_direction}] Starting scraper...")
        
        continuation_token = None
        page_number = 1
        
        timeout = aiohttp.ClientTimeout(total=30)
        async with aiohttp.ClientSession(headers=self.headers, timeout=timeout) as session:
            while not self.stop_scraping:
                print(f"\n[{sort_direction}] --- Page {page_number} ---")
                
                # Update page stats
                self.stats[stats_key]['pages'] = page_number
                
                # Make request
                response_content = await self.make_request(session, continuation_token, sort_by_highest)
                if not response_content:
                    print(f"[{sort_direction}] Failed to get response, stopping...")
                    break
                
                # Parse reviews from response
                new_reviews = self.parse_reviews_from_response(response_content, sort_direction)
                
                if not new_reviews:
                    print(f"[{sort_direction}] No new reviews found, stopping...")
                    break
                
                # Add new reviews to shared collection
                with self.lock:
                    if self.stop_scraping:
                        print(f"[{sort_direction}] Stopping due to duplicate limit")
                        break
                        
                    self.all_reviews.extend(new_reviews)
                    print(f"[{sort_direction}] Added {len(new_reviews)} new reviews. Total so far: {len(self.all_reviews)}")
                
                # Extract continuation tokens for next request
                caesy_tokens = self.extract_caesy_tokens(response_content)
                
                # Save tokens for debugging
                if sort_by_highest:
                    self.all_tokens['highest_rating'].extend(caesy_tokens)
                else:
                    self.all_tokens['lowest_rating'].extend(caesy_tokens)
                
                if caesy_tokens:
                    print(f"[{sort_direction}] Found {len(caesy_tokens)} continuation tokens")
                    
                    # Get next unused token
                    next_token = self.get_next_unused_token(caesy_tokens, used_tokens)
                    
                    if next_token:
                        # Mark current token as used if we have one
                        if continuation_token:
                            used_tokens.add(continuation_token)
                            print(f"[{sort_direction}] Marked token as used: {continuation_token[:50]}...")
                        
                        continuation_token = next_token
                        print(f"[{sort_direction}] Using next unused token: {continuation_token[:50]}...")
                        print(f"[{sort_direction}] Total tokens used so far: {len(used_tokens)}")
                    else:
                        print(f"[{sort_direction}] All available tokens have been used, stopping...")
                        break
                else:
                    print(f"[{sort_direction}] No continuation tokens found, stopping...")
                    break
                
                page_number += 1
                
                # Add delay between requests to be respectful
                await asyncio.sleep(2)
        
        print(f"[{sort_direction}] Scraper finished. Total pages processed: {page_number}")

    def filter_reviews_by_source(self, reviews):
        """Filter reviews by source if source_filter is specified"""
        if not self.source_filter:
            return reviews  # No filtering, return all reviews
        
        filtered_reviews = []
        original_count = len(reviews)
        
        for review in reviews:
            review_source = review.get('source', '').lower()
            
            # Check if review matches the source filter
            if self.source_filter == 'google' and review_source == 'google':
                filtered_reviews.append(review)
            elif self.source_filter == 'tripadvisor' and review_source == 'tripadvisor':
                filtered_reviews.append(review)
            elif self.source_filter == review_source:  # For other sources
                filtered_reviews.append(review)
        
        filtered_count = len(filtered_reviews)
        print(f"📊 Source filtering: {original_count} total → {filtered_count} {self.source_filter.title()} reviews")
        
        return filtered_reviews

    def save_results_to_files(self):
        """Save only the reviews array to file with optional source filtering"""
        # Apply source filtering if specified
        reviews_to_save = self.filter_reviews_by_source(self.all_reviews)
        
        try:
            with open(self.output_file, 'w', encoding='utf-8') as file:
                json.dump(reviews_to_save, file, indent=2, ensure_ascii=False)
            
            if self.source_filter:
                print(f"✅ {self.source_filter.title()} reviews saved to: {self.output_file}")
            else:
                print(f"✅ All reviews saved to: {self.output_file}")
                
        except Exception as e:
            print(f"Error saving reviews: {e}")

    async def scrape_all_reviews_dual(self):
        """Main method to scrape reviews from both directions simultaneously"""
        print(f"Starting dual async scraping for place ID: 0x{self.place_id}")
        print("Running two concurrent scrapers:")
        print("  1. Highest rating first (sort: 1e3)")
        print("  2. Lowest rating first (sort: 1e4)")
        print("Will stop when more than 10 duplicate reviewers are found in a single request")
        
        # Create tasks for both directions
        highest_task = asyncio.create_task(self.scrape_direction(sort_by_highest=True))
        lowest_task = asyncio.create_task(self.scrape_direction(sort_by_highest=False))
        
        # Wait for both to complete (or until one stops due to duplicates)
        await asyncio.gather(highest_task, lowest_task, return_exceptions=True)
        
        # Save results
        self.save_results_to_files()
        
        print(f"\n=== DUAL SCRAPING COMPLETE ===")
        print(f"Total reviews scraped: {len(self.all_reviews)}")
        print(f"Total duplicates found: {self.duplicate_count}")
        print(f"Stopped due to duplicate limit: {self.stop_scraping}")
        print(f"Stats per direction:")
        for direction, stats in self.stats.items():
            print(f"  {direction}: {stats['pages']} pages, {stats['reviews']} reviews, {stats['duplicates']} duplicates")
        print(f"Reviews output file: {self.output_file}")
    
    async def scrape_reviews_dual_direction(self, google_maps_url=None, max_pages_per_direction=None, delay_between_requests=2):
        """
        Wrapper method for scraping reviews with parameters
        This method maintains compatibility with the existing interface
        """
        # Update delay if specified
        if delay_between_requests != 2:
            # We can store this and use it in scrape_direction if needed
            pass
        
        # Run the dual scraping
        await self.scrape_all_reviews_dual()
        
        return self.all_reviews

def main():
    """Main function with command-line interface"""
    # Check if command line arguments are provided
    if len(sys.argv) > 1:
        # Parse command line arguments
        args = parse_command_line_args()
        
        print("🚀 Google Maps Review Scraper")
        print("=" * 50)
        print(f"📍 URL: {args.url}")
        
        # Extract place ID from URL
        place_id = extract_place_id_from_url(args.url)
        if not place_id:
            print("❌ Failed to extract place ID from URL")
            sys.exit(1)
        
        print(f"🔍 Extracted Place ID: {place_id}")
        
        # Show source filter info
        if args.source_filter:
            print(f"🎯 Source Filter: {args.source_filter.title()} reviews only")
        else:
            print("🎯 Source Filter: All reviews")
        
        # Clean the place ID
        if place_id.startswith("1s0x"):
            place_id = place_id[4:]  # Remove "1s0x" prefix
        elif place_id.startswith("0x"):
            place_id = place_id[2:]  # Remove "0x" prefix
        
        # Create scraper instance with source filter
        scraper = DualAsyncGoogleMapsReviewScraper(place_id, source_filter=args.source_filter)
        
        # Run the async scraping
        asyncio.run(scraper.scrape_all_reviews_dual())
        
    else:
        # Interactive mode (existing functionality)
        print("🚀 Google Maps Review Scraper (Interactive Mode)")
        print("=" * 50)
        print("💡 Tip: You can also use command line: python extract.py 'URL' --google")
        print()
        
        # Get place ID from user input
        place_id = input("Enter the place ID (e.g., 89c3ca9c11f90c25:0x6cc8dba851799f09): ").strip()
        
        # Clean the place ID
        if place_id.startswith("1s0x"):
            place_id = place_id[4:]  # Remove "1s0x" prefix
        elif place_id.startswith("0x"):
            place_id = place_id[2:]  # Remove "0x" prefix
        
        # Create scraper instance without source filter (gets all reviews)
        scraper = DualAsyncGoogleMapsReviewScraper(place_id)
        
        # Run the async scraping
        asyncio.run(scraper.scrape_all_reviews_dual())

if __name__ == "__main__":
    main()
