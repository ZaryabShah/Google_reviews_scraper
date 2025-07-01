import json
import re
from urllib.parse import unquote
from datetime import datetime
import traceback

def extract_caesy_tokens(html_content):
    """Extract all tokens starting with CAESY0"""
    # Find all tokens that start with CAESY0
    caesy_tokens = re.findall(r'CAESY0[A-Za-z0-9_\-+=]{10,}', html_content)
    
    # Remove duplicates while preserving order
    unique_tokens = []
    seen = set()
    for token in caesy_tokens:
        if token not in seen:
            unique_tokens.append(token)
            seen.add(token)
    
    return unique_tokens

def parse_timestamp(timestamp_microseconds):
    """Convert microsecond timestamp to ISO format"""
    try:
        if timestamp_microseconds:
            # Convert microseconds to seconds
            timestamp_seconds = int(timestamp_microseconds) / 1000000
            dt = datetime.fromtimestamp(timestamp_seconds)
            return dt.isoformat()
    except:
        pass
    return None

def extract_place_id_and_coordinates(html_content):
    """Extract place ID and coordinates from the response"""
    place_data = {}
    
    # Extract place ID (hex format)
    place_id_match = re.search(r'"0x0:(0x[a-f0-9]+)"', html_content)
    if place_id_match:
        place_data['place_id'] = place_id_match.group(1)
    
    # Default coordinates - could be enhanced to extract actual coordinates
    place_data['latitude'] = 40.0
    place_data['longitude'] = 40.0
    
    return place_data

def parse_json_structure(html_content):
    """Parse the JSON structure from Google Maps response"""
    try:
        # Remove the security prefix
        if html_content.startswith(")]}'\n"):
            json_str = html_content[5:]
        else:
            json_str = html_content
        
        # Parse the main JSON structure
        data = json.loads(json_str)
        
        # Navigate to the reviews array - it's typically in the 3rd element
        if len(data) >= 3 and data[2] and isinstance(data[2], list):
            return data[2]
        
        return []
    except Exception as e:
        print(f"Error parsing JSON structure: {e}")
        return []

def extract_reviews_from_json(reviews_data):
    """Extract review information from parsed JSON data"""
    reviews = []
    
    for review_item in reviews_data:
        try:
            if not isinstance(review_item, list) or len(review_item) < 2:
                continue
                
            review_data = review_item[0]  # First element contains review details
            if not isinstance(review_data, list) or len(review_data) < 2:
                continue
            
            # Extract basic review information
            review_id = review_data[0] if len(review_data) > 0 else ""
            
            # Skip if this doesn't look like a review ID
            if not review_id or not review_id.startswith(("ChZDSUhN", "ChdDSUhN")):
                continue
            
            review_details = review_data[1] if len(review_data) > 1 else []
            if not isinstance(review_details, list):
                continue
            
            # Extract place ID and timestamps
            place_id = ""
            published_timestamp = None
            edited_timestamp = None
            
            if len(review_details) > 0:
                place_id = review_details[0] if review_details[0] else ""
            if len(review_details) > 2:
                published_timestamp = review_details[2]
            if len(review_details) > 3:
                edited_timestamp = review_details[3]
            
            # Extract reviewer information
            reviewer_info = review_details[4] if len(review_details) > 4 else []
            reviewer_name = ""
            reviewer_photo = ""
            reviewer_url = ""
            reviewer_id = ""
            reviewer_count = 0
            
            if isinstance(reviewer_info, list) and len(reviewer_info) > 5:
                reviewer_details = reviewer_info[5]
                if isinstance(reviewer_details, list) and len(reviewer_details) > 0:
                    reviewer_name = reviewer_details[0] if reviewer_details[0] else ""
                    if len(reviewer_details) > 1:
                        reviewer_photo = reviewer_details[1] if reviewer_details[1] else ""
                    if len(reviewer_details) > 2 and isinstance(reviewer_details[2], list) and len(reviewer_details[2]) > 0:
                        reviewer_url = reviewer_details[2][0] if reviewer_details[2][0] else ""
                    if len(reviewer_details) > 3:
                        reviewer_id = reviewer_details[3] if reviewer_details[3] else ""
                    if len(reviewer_details) > 5:
                        reviewer_count = reviewer_details[5] if isinstance(reviewer_details[5], int) else 0
            
            # Extract time ago and rating information
            time_ago = ""
            rating = 5  # Default rating
            
            # Time ago is usually in review_item[1][6] or similar position
            if len(review_item) > 1 and isinstance(review_item[1], list):
                additional_info = review_item[1]
                if len(additional_info) > 6:
                    time_ago = additional_info[6] if additional_info[6] else ""
                
                # Rating is often in the first sub-array
                if len(additional_info) > 0 and isinstance(additional_info[0], list):
                    rating_info = additional_info[0]
                    if len(rating_info) > 0 and isinstance(rating_info[0], int):
                        rating = rating_info[0]
            
            # Extract review text
            review_text = ""
            # Review text is often deeply nested in the structure
            for item in review_item:
                if isinstance(item, list):
                    text_found = extract_text_from_nested_structure(item)
                    if text_found and len(text_found) > 20:  # Only consider substantial text
                        review_text = text_found
                        break
            
            # Build the review object
            review = {
                "reviewerId": reviewer_id,
                "reviewerUrl": reviewer_url,
                "reviewerName": reviewer_name,
                "reviewerNumberOfReviews": reviewer_count,
                "reviewerPhotoUrl": reviewer_photo,
                "text": review_text,
                "reviewImageUrls": [],
                "publishedAtDate": parse_timestamp(published_timestamp),
                "lastEditedAtDate": parse_timestamp(edited_timestamp),
                "likesCount": 0,
                "reviewId": review_id,
                "reviewUrl": f"https://www.google.com/maps/reviews/data=!4m8!14m7!1m6!2m5!1s{review_id}" if review_id else "",
                "stars": rating,
                "placeId": place_id,
                "location": {
                    "lat": 40.0,  # Could be enhanced to extract actual coordinates
                    "lng": 40.0
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
                "cid": place_id,
                "fid": "",
                "scrapedAt": datetime.now().isoformat(),
                "timeAgo": time_ago
            }
            
            reviews.append(review)
            
        except Exception as e:
            print(f"Error processing review item: {e}")
            continue
    
    return reviews

def extract_text_from_nested_structure(data, max_depth=10, current_depth=0):
    """Recursively extract text from nested list/dict structures"""
    if current_depth > max_depth:
        return ""
    
    if isinstance(data, str):
        return data.strip()
    elif isinstance(data, list):
        for item in data:
            if isinstance(item, str) and len(item.strip()) > 20:
                return item.strip()
            elif isinstance(item, list):
                result = extract_text_from_nested_structure(item, max_depth, current_depth + 1)
                if result and len(result) > 20:
                    return result
    
    return ""

def extract_reviews_data_enhanced(html_content):
    """Enhanced extraction using JSON parsing"""
    place_data = extract_place_id_and_coordinates(html_content)
    
    # Try JSON parsing first
    json_reviews = parse_json_structure(html_content)
    if json_reviews:
        reviews = extract_reviews_from_json(json_reviews)
        if reviews:
            # Update place information for all reviews
            for review in reviews:
                if not review.get('placeId'):
                    review['placeId'] = place_data.get('place_id', '')
                if not review.get('cid'):
                    review['cid'] = place_data.get('place_id', '')
                review['location'] = {
                    "lat": place_data.get('latitude', 40.0),
                    "lng": place_data.get('longitude', 40.0)
                }
            return reviews
    
    # Fallback to regex parsing if JSON parsing fails
    return extract_reviews_regex_fallback(html_content, place_data)

def extract_reviews_regex_fallback(html_content, place_data):
    """Fallback regex-based extraction method"""
    reviews = []
    
    try:
        # Extract all review IDs
        review_ids = re.findall(r'"(Ch[dZ]DSUH[A-Za-z0-9]{20,})"', html_content)
        
        # Extract all reviewer names (look for quoted names in the content)
        reviewer_names = re.findall(r'"([A-Za-z][A-Za-z\s\.]+)"', html_content)
        # Filter names that look like actual people names
        reviewer_names = [name for name in reviewer_names if 
                         len(name.split()) <= 4 and 
                         not any(char in name for char in ['http', '.com', '@', '_']) and
                         len(name) > 2]
        
        # Extract reviewer IDs (21-digit numbers)
        reviewer_ids = re.findall(r'"(\d{21})"', html_content)
        
        # Extract profile image URLs
        profile_images = re.findall(r'"(https://lh3\.googleusercontent\.com/[^"]+)"', html_content)
        
        # Extract timestamps
        timestamps = re.findall(r'(\d{13,})', html_content)
        
        # Extract time ago patterns
        time_ago_patterns = re.findall(r'"((?:\d+\s+(?:years?|months?|weeks?|days?|hours?|minutes?)\s+ago|Edited\s+[^"]+))"', html_content)
        
        # Extract review texts using a more comprehensive approach
        review_texts = []
        
        # Look for text patterns that are likely to be reviews
        text_patterns = [
            r'"([^"]{50,500})"',  # Text between 50-500 characters
        ]
        
        potential_texts = []
        for pattern in text_patterns:
            matches = re.findall(pattern, html_content)
            for match in matches:
                # Filter out URLs, technical strings, etc.
                if (not any(x in match.lower() for x in ['http', '.com', 'maps', 'google', 'business', 'reviews']) and
                    not match.startswith(('Ch', 'CG', 'CA')) and
                    len(match.split()) > 5):  # At least 5 words
                    potential_texts.append(match)
        
        # Take unique texts and limit to reasonable review length
        seen_texts = set()
        for text in potential_texts:
            if text not in seen_texts and 20 <= len(text) <= 1000:
                review_texts.append(text)
                seen_texts.add(text)
            if len(review_texts) >= len(review_ids):
                break
        
        # Extract star ratings (look for rating patterns)
        ratings = []
        rating_patterns = re.findall(r'\[\[(\d)\]\]', html_content)
        ratings.extend([int(r) for r in rating_patterns if 1 <= int(r) <= 5])
        
        # Extract reviewer review counts
        reviewer_counts = re.findall(r'(\d+)\s+reviews?', html_content)
        reviewer_counts = [int(count) for count in reviewer_counts if count.isdigit() and 1 <= int(count) <= 10000]
        
        # Build reviews from extracted data
        max_reviews = min(len(review_ids), 50)  # Limit to 50 reviews max
        
        for i in range(max_reviews):
            review = {
                "reviewerId": reviewer_ids[i] if i < len(reviewer_ids) else f"reviewer_{i}",
                "reviewerUrl": f"https://www.google.com/maps/contrib/{reviewer_ids[i]}?hl=en" if i < len(reviewer_ids) else "",
                "reviewerName": reviewer_names[i] if i < len(reviewer_names) else f"Reviewer {i+1}",
                "reviewerNumberOfReviews": reviewer_counts[i] if i < len(reviewer_counts) else 0,
                "reviewerPhotoUrl": profile_images[i] if i < len(profile_images) else "",
                "text": review_texts[i] if i < len(review_texts) else "",
                "reviewImageUrls": [],
                "publishedAtDate": parse_timestamp(timestamps[i*2]) if i*2 < len(timestamps) else datetime.now().isoformat(),
                "lastEditedAtDate": parse_timestamp(timestamps[i*2+1]) if i*2+1 < len(timestamps) else None,
                "likesCount": 0,
                "reviewId": review_ids[i] if i < len(review_ids) else f"review_{i}",
                "reviewUrl": f"https://www.google.com/maps/reviews/data=!4m8!14m7!1m6!2m5!1s{review_ids[i]}" if i < len(review_ids) else "",
                "stars": ratings[i] if i < len(ratings) else 5,
                "placeId": place_data.get('place_id', ''),
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
                "timeAgo": time_ago_patterns[i] if i < len(time_ago_patterns) else ""
            }
            reviews.append(review)
            
    except Exception as e:
        print(f"Error in regex fallback extraction: {e}")
        traceback.print_exc()
    
    return reviews

def main():
    # Read the HTML file
    try:
        with open('response2.html', 'r', encoding='utf-8') as file:
            html_content = file.read()
    except FileNotFoundError:
        print("Error: response2.html file not found!")
        return
    except Exception as e:
        print(f"Error reading HTML file: {e}")
        return
    
    # Extract CAESY tokens
    print("Extracting CAESY tokens...")
    caesy_tokens = extract_caesy_tokens(html_content)
    
    # Extract reviews using enhanced method
    print("Extracting reviews data with enhanced parser...")
    reviews_data = extract_reviews_data_enhanced(html_content)
    
    # Prepare tokens data
    tokens_data = {
        'extraction_timestamp': datetime.now().isoformat(),
        'total_tokens_found': len(caesy_tokens),
        'tokens': caesy_tokens
    }
    
    # Prepare reviews data
    reviews_only_data = {
        'extraction_timestamp': datetime.now().isoformat(),
        'reviews': reviews_data,
        'total_reviews_found': len(reviews_data),
        'parser_version': 'enhanced_v2',
        'note': 'Enhanced parser that extracts all available reviews from JSON structure'
    }
    
    # Generate timestamp for file names
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Save tokens to JSON file
    tokens_filename = f'caesy_tokens_enhanced_{timestamp}.json'
    try:
        with open(tokens_filename, 'w', encoding='utf-8') as file:
            json.dump(tokens_data, file, indent=2, ensure_ascii=False)
        print(f"✅ Tokens saved to: {tokens_filename}")
        print(f"   Total CAESY tokens found: {len(caesy_tokens)}")
    except Exception as e:
        print(f"Error saving tokens: {e}")
    
    # Save reviews data to JSON file
    reviews_filename = f'reviews_enhanced_{timestamp}.json'
    try:
        with open(reviews_filename, 'w', encoding='utf-8') as file:
            json.dump(reviews_only_data, file, indent=2, ensure_ascii=False)
        print(f"✅ Enhanced reviews data saved to: {reviews_filename}")
        print(f"   Total reviews found: {len(reviews_data)}")
    except Exception as e:
        print(f"Error saving reviews data: {e}")
    
    # Print summary
    print("\n=== ENHANCED EXTRACTION SUMMARY ===")
    print(f"CAESY Tokens: {len(caesy_tokens)}")
    print(f"Total Reviews: {len(reviews_data)}")
    
    if reviews_data:
        print("\nFirst 3 Reviews Summary:")
        for i, review in enumerate(reviews_data[:3]):
            print(f"\n  Review {i+1}:")
            print(f"    Reviewer: {review.get('reviewerName', 'N/A')}")
            print(f"    Reviewer ID: {review.get('reviewerId', 'N/A')}")
            print(f"    Rating: {review.get('stars', 'N/A')}")
            print(f"    Time: {review.get('timeAgo', 'N/A')}")
            print(f"    Review ID: {review.get('reviewId', 'N/A')}")
            print(f"    Text: {review.get('text', 'N/A')[:100]}...")
        
        print(f"\nAll Review IDs found:")
        for i, review in enumerate(reviews_data):
            print(f"  {i+1:2d}. {review.get('reviewId', 'N/A')}")
    
    if caesy_tokens:
        print(f"\nSample CAESY Token: {caesy_tokens[0]}")

if __name__ == "__main__":
    main()
