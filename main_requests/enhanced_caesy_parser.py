"""
Enhanced Google Maps Reviews Parser using CAESY tokens
Extracts comprehensive review data from HTML files with improved pattern matching
"""

import re
import json
import base64
from typing import List, Dict, Any, Optional
from urllib.parse import unquote
import sys
from datetime import datetime


class EnhancedCaesyParser:
    def __init__(self, html_content: str):
        """Initialize parser with HTML content"""
        self.html_content = html_content
        self.reviews = []
        self.location_info = {}
        
    def find_caesy_tokens(self) -> List[str]:
        """Find all CAESY tokens in the HTML content"""
        # Pattern to match CAESY tokens 
        pattern = r'"(CAESY[^"]*)"'
        tokens = re.findall(pattern, self.html_content)
        return tokens
    
    def extract_review_sections(self) -> List[str]:
        """Split content by CAESY tokens to get individual review sections"""
        tokens = self.find_caesy_tokens()
        if not tokens:
            return []
            
        sections = []
        content = self.html_content
        
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
    
    def extract_star_rating(self, section: str) -> Optional[int]:
        """Extract star rating with precise pattern matching for Google Maps structure"""
        # Primary pattern: [[N], where N is the star rating at the start of review data
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
        
        # Fallback patterns if primary doesn't work
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
    
    def extract_likes_count(self, section: str) -> Optional[int]:
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
    
    def extract_user_info(self, section: str) -> Dict[str, Any]:
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
    
    def extract_review_text(self, section: str) -> List[str]:
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
    
    def extract_date_info(self, section: str) -> Dict[str, Any]:
        """Extract comprehensive date information"""
        date_info = {}
        
        # Patterns for relative dates
        relative_patterns = [
            r'"(\d+)\s*years?\s*ago"',
            r'"(\d+)\s*months?\s*ago"',
            r'"(\d+)\s*weeks?\s*ago"',
            r'"(\d+)\s*days?\s*ago"',
            r'"(a\s*year\s*ago)"',
            r'"(a\s*month\s*ago)"',
            r'"(Edited[^"]*)"',
        ]
        
        for pattern in relative_patterns:
            matches = re.findall(pattern, section)
            if matches:
                date_info['relative_date'] = matches[0]
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
    
    def extract_business_info(self, section: str) -> Dict[str, Any]:
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
    
    def extract_review_images(self, section: str) -> List[str]:
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
    
    def extract_review_features(self, section: str) -> Dict[str, Any]:
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
    
    def extract_owner_response(self, texts: List[str]) -> Optional[str]:
        """Identify owner response from multiple texts"""
        if len(texts) > 1:
            # Usually the second text is the owner response
            # Can also check for common owner response patterns
            for i, text in enumerate(texts[1:], 1):
                if any(word in text.lower() for word in ['thank', 'appreciate', 'glad', 'sorry', 'pleasure']):
                    return text
            return texts[1]  # Default to second text
        return None
    
    def extract_single_review(self, section: str) -> Dict[str, Any]:
        """Extract comprehensive data for a single review"""
        review = {}
        
        # Basic review data
        review['rating'] = self.extract_star_rating(section)
        review['likes_count'] = self.extract_likes_count(section)
        review['user_info'] = self.extract_user_info(section)
        review['date_info'] = self.extract_date_info(section)
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
    
    def parse_all_reviews(self) -> List[Dict[str, Any]]:
        """Parse all reviews with enhanced validation"""
        sections = self.extract_review_sections()
        print(f"Found {len(sections)} review sections")
        
        reviews = []
        
        for i, section in enumerate(sections):
            try:
                review = self.extract_single_review(section)
                
                # Enhanced validation - require at least one meaningful field
                has_user = bool(review.get('user_info', {}).get('name'))
                has_text = bool(review.get('review_text'))
                has_rating = review.get('rating') is not None
                has_date = bool(review.get('date_info'))
                
                if has_user or has_text or has_rating or has_date:
                    review['section_index'] = i
                    review['extraction_confidence'] = self.calculate_confidence(review)
                    reviews.append(review)
                    
                    user_name = review.get('user_info', {}).get('name', 'Unknown')
                    rating = review.get('rating', 'N/A')
                    print(f"Extracted review {len(reviews)}: {user_name} (Rating: {rating})")
                
            except Exception as e:
                print(f"Error parsing section {i}: {str(e)}")
                continue
        
        return reviews
    
    def calculate_confidence(self, review: Dict[str, Any]) -> float:
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
    
    def save_reviews(self, output_file: str) -> List[Dict[str, Any]]:
        """Save parsed reviews with metadata"""
        reviews = self.parse_all_reviews()
        
        # Calculate statistics
        ratings = [r['rating'] for r in reviews if r.get('rating') is not None]
        avg_rating = sum(ratings) / len(ratings) if ratings else None
        
        output_data = {
            'metadata': {
                'total_reviews': len(reviews),
                'extraction_timestamp': datetime.now().isoformat(),
                'average_rating': avg_rating,
                'rating_distribution': {str(i): ratings.count(i) for i in range(1, 6)},
                'reviews_with_images': len([r for r in reviews if r.get('has_images')]),
                'reviews_with_owner_response': len([r for r in reviews if r.get('has_owner_response')]),
                'avg_confidence': sum(r.get('extraction_confidence', 0) for r in reviews) / len(reviews) if reviews else 0
            },
            'reviews': reviews
        }
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, indent=2, ensure_ascii=False)
        
        print(f"\nSaved {len(reviews)} reviews to {output_file}")
        print(f"Average rating: {avg_rating:.1f}" if avg_rating else "No ratings found")
        print(f"Reviews with images: {output_data['metadata']['reviews_with_images']}")
        print(f"Reviews with owner responses: {output_data['metadata']['reviews_with_owner_response']}")
        
        return reviews


def parse_html_file(html_file_path: str, output_file: str = None) -> List[Dict[str, Any]]:
    """Parse reviews from HTML file with enhanced parser"""
    try:
        with open(html_file_path, 'r', encoding='utf-8') as f:
            html_content = f.read()
        
        parser = EnhancedCaesyParser(html_content)
        
        if output_file:
            reviews = parser.save_reviews(output_file)
        else:
            reviews = parser.parse_all_reviews()
        
        return reviews
        
    except Exception as e:
        print(f"Error parsing HTML file: {str(e)}")
        return []


def main():
    """Main function for command line usage"""
    if len(sys.argv) < 2:
        print("Usage: python enhanced_caesy_parser.py <html_file> [output_file]")
        sys.exit(1)
    
    html_file = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else None
    
    if not output_file:
        # Auto-generate output filename
        import os
        base_name = os.path.splitext(os.path.basename(html_file))[0]
        output_file = f"enhanced_reviews_{base_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    
    reviews = parse_html_file(html_file, output_file)
    
    if reviews:
        sample_review = reviews[0]
        print(f"\nSample review:")
        print(f"User: {sample_review.get('user_info', {}).get('name', 'Unknown')}")
        print(f"Rating: {sample_review.get('rating', 'Unknown')}")
        print(f"Likes: {sample_review.get('likes_count', 'Unknown')}")
        print(f"Date: {sample_review.get('date_info', {}).get('relative_date', 'Unknown')}")
        print(f"Text preview: {sample_review.get('review_text', 'No text')[:100]}...")
        print(f"Confidence: {sample_review.get('extraction_confidence', 0):.2f}")


if __name__ == "__main__":
    main()
