"""
Universal Google Maps Reviews Parser
Automatically detects and parses reviews from various Google Maps HTML structures
"""

import re
import json
import base64
from typing import List, Dict, Any, Optional, Union
from urllib.parse import unquote
import sys
from datetime import datetime


class UniversalGoogleMapsParser:
    def __init__(self, html_content: str):
        """Initialize universal parser"""
        self.html_content = html_content
        self.parser_type = self.detect_parser_type()
        
    def detect_parser_type(self) -> str:
        """Detect the type of HTML structure"""
        if 'CAESY' in self.html_content:
            return 'caesy'
        elif '"reviews"' in self.html_content.lower():
            return 'json_embedded'
        elif 'data-review-id' in self.html_content:
            return 'dom_structured'
        else:
            return 'generic'
    
    def parse_reviews(self) -> List[Dict[str, Any]]:
        """Parse reviews using the detected method"""
        if self.parser_type == 'caesy':
            return self.parse_caesy_reviews()
        elif self.parser_type == 'json_embedded':
            return self.parse_json_embedded()
        elif self.parser_type == 'dom_structured':
            return self.parse_dom_structured()
        else:
            return self.parse_generic()
    
    def parse_caesy_reviews(self) -> List[Dict[str, Any]]:
        """Parse CAESY token based reviews"""
        sections = self.extract_caesy_sections()
        reviews = []
        
        for i, section in enumerate(sections):
            try:
                review = self.extract_caesy_review(section, i)
                if self.is_valid_review(review):
                    reviews.append(review)
            except Exception as e:
                print(f"Error parsing CAESY section {i}: {e}")
                continue
        
        return reviews
    
    def extract_caesy_sections(self) -> List[str]:
        """Extract sections using CAESY tokens"""
        caesy_pattern = r'"(CAESY[^"]*)"'
        tokens = re.findall(caesy_pattern, self.html_content)
        
        if not tokens:
            return []
        
        sections = []
        content = self.html_content
        
        # Find all token positions
        positions = []
        for token in tokens:
            pos = content.find(f'"{token}"')
            if pos != -1:
                positions.append(pos)
        
        positions.sort()
        
        # Extract sections
        for i, pos in enumerate(positions):
            if i + 1 < len(positions):
                section = content[pos:positions[i + 1]]
            else:
                section = content[pos:]
            sections.append(section)
        
        return sections
    
    def extract_caesy_review(self, section: str, index: int) -> Dict[str, Any]:
        """Extract review data from CAESY section"""
        review = {'section_index': index, 'parser_type': 'caesy'}
        
        # User information
        review['user_info'] = self.extract_user_info_caesy(section)
        
        # Review content
        review['review_text'] = self.extract_review_text_caesy(section)
        review['owner_response'] = self.extract_owner_response_caesy(section)
        
        # Review metadata
        review['rating'] = self.extract_rating_caesy(section)
        review['likes_count'] = self.extract_likes_caesy(section)
        review['date_info'] = self.extract_date_caesy(section)
        
        # Location and business
        review['business_info'] = self.extract_business_info_caesy(section)
        
        # Media
        review['review_images'] = self.extract_images_caesy(section)
        
        # Additional features
        review['features'] = self.extract_features_caesy(section)
        
        return review
    
    def extract_user_info_caesy(self, section: str) -> Dict[str, Any]:
        """Extract user info from CAESY section"""
        user_info = {}
        
        # Name patterns
        name_patterns = [
            r'"([^"]+)","https://lh3\.googleusercontent\.com/a[^"]*"',
            r'\["([^"]+)","https://lh3\.googleusercontent\.com',
        ]
        
        for pattern in name_patterns:
            matches = re.findall(pattern, section)
            if matches:
                user_info['name'] = matches[0]
                break
        
        # Profile image
        profile_img_pattern = r'"(https://lh3\.googleusercontent\.com/a[^"]*(?:s120-c-rp|br100)[^"]*)"'
        profile_matches = re.findall(profile_img_pattern, section)
        if profile_matches:
            user_info['profile_image'] = profile_matches[0]
        
        # User ID
        user_id_pattern = r'"(\d{21})"'
        user_id_matches = re.findall(user_id_pattern, section)
        if user_id_matches:
            user_info['user_id'] = user_id_matches[0]
        
        # Review count
        review_count_pattern = r'"(\d+)\s*reviews?"'
        review_count_matches = re.findall(review_count_pattern, section)
        if review_count_matches:
            user_info['review_count'] = int(review_count_matches[0])
        
        # Local guide
        user_info['is_local_guide'] = 'Local Guide' in section
        
        return user_info
    
    def extract_review_text_caesy(self, section: str) -> Optional[str]:
        """Extract main review text"""
        text_patterns = [
            r'\["([^"]{20,})",null,\[0,\d+\]\]',
            r'"text":"([^"]{20,})"',
        ]
        
        for pattern in text_patterns:
            matches = re.findall(pattern, section)
            for text in matches:
                if self.is_review_text(text):
                    return self.clean_text(text)
        
        return None
    
    def extract_owner_response_caesy(self, section: str) -> Optional[str]:
        """Extract owner response"""
        # Look for multiple text blocks, second one is usually owner response
        text_pattern = r'\["([^"]{10,})",null,\[0,\d+\]\]'
        texts = re.findall(text_pattern, section)
        
        cleaned_texts = []
        for text in texts:
            if self.is_review_text(text):
                cleaned_texts.append(self.clean_text(text))
        
        if len(cleaned_texts) > 1:
            # Check if second text looks like owner response
            second_text = cleaned_texts[1]
            if any(word in second_text.lower() for word in ['thank', 'appreciate', 'glad', 'sorry', 'pleasure']):
                return second_text
            return second_text
        
        return None
    
    def extract_rating_caesy(self, section: str) -> Optional[int]:
        """Extract star rating"""
        rating_patterns = [
            r'\[\[(\d)\]\]',
            r'"rating":(\d)',
            r'"stars":(\d)',
        ]
        
        for pattern in rating_patterns:
            matches = re.findall(pattern, section)
            for match in matches:
                rating = int(match)
                if 1 <= rating <= 5:
                    return rating
        
        return None
    
    def extract_likes_caesy(self, section: str) -> Optional[int]:
        """Extract likes/helpful count"""
        likes_patterns = [
            r'\[\[1,(\d+)\]\]',
            r'"helpful_count":(\d+)',
        ]
        
        for pattern in likes_patterns:
            matches = re.findall(pattern, section)
            if matches:
                return int(matches[-1])
        
        return None
    
    def extract_date_caesy(self, section: str) -> Dict[str, Any]:
        """Extract date information"""
        date_info = {}
        
        # Relative date
        relative_patterns = [
            r'"(\d+\s*years?\s*ago)"',
            r'"(\d+\s*months?\s*ago)"',
            r'"(\d+\s*days?\s*ago)"',
            r'"(Edited[^"]*)"',
        ]
        
        for pattern in relative_patterns:
            matches = re.findall(pattern, section)
            if matches:
                date_info['relative_date'] = matches[0]
                break
        
        # Timestamp
        timestamp_pattern = r'(\d{13})'
        timestamp_matches = re.findall(timestamp_pattern, section)
        if timestamp_matches:
            try:
                timestamp = int(timestamp_matches[0]) / 1000
                date_info['timestamp'] = timestamp
                date_info['iso_date'] = datetime.fromtimestamp(timestamp).isoformat()
            except:
                pass
        
        return date_info
    
    def extract_business_info_caesy(self, section: str) -> Dict[str, Any]:
        """Extract business information"""
        business_info = {}
        
        # Business ID
        business_id_pattern = r'"(0x0:0x[a-f0-9]+)"'
        business_matches = re.findall(business_id_pattern, section)
        if business_matches:
            business_info['business_id'] = business_matches[0]
        
        # Coordinates
        coord_pattern = r'\[3,(-?\d+\.?\d*),(-?\d+\.?\d*)\]'
        coord_matches = re.findall(coord_pattern, section)
        if coord_matches:
            lng, lat = coord_matches[0]
            business_info['coordinates'] = {
                'latitude': float(lat),
                'longitude': float(lng)
            }
        
        return business_info
    
    def extract_images_caesy(self, section: str) -> List[str]:
        """Extract review images"""
        image_patterns = [
            r'"(https://lh3\.googleusercontent\.com/geougc-cs/[^"]+)"',
            r'"(https://lh3\.googleusercontent\.com/places/[^"]+)"',
        ]
        
        images = []
        for pattern in image_patterns:
            matches = re.findall(pattern, section)
            images.extend(matches)
        
        return list(set(images))  # Remove duplicates
    
    def extract_features_caesy(self, section: str) -> Dict[str, Any]:
        """Extract additional features"""
        features = {}
        
        # Service type
        if 'TAKE_OUT' in section:
            features['service_type'] = 'takeout'
        elif 'DINE_IN' in section:
            features['service_type'] = 'dine_in'
        elif 'DELIVERY' in section:
            features['service_type'] = 'delivery'
        
        # Price range
        price_pattern = r'USD_(\d+)_TO_(\d+)'
        price_matches = re.findall(price_pattern, section)
        if price_matches:
            min_price, max_price = price_matches[0]
            features['price_range'] = f"${min_price}-{max_price}"
        
        # Recommended dishes
        dish_pattern = r'"([^"]+)","M:/g/[^"]*"'
        dish_matches = re.findall(dish_pattern, section)
        if dish_matches:
            features['recommended_dishes'] = dish_matches
        
        return features
    
    def parse_json_embedded(self) -> List[Dict[str, Any]]:
        """Parse JSON embedded reviews"""
        # Implementation for JSON embedded format
        reviews = []
        # This would be implemented based on specific JSON structure
        return reviews
    
    def parse_dom_structured(self) -> List[Dict[str, Any]]:
        """Parse DOM structured reviews"""
        # Implementation for DOM structured format
        reviews = []
        # This would use BeautifulSoup or similar for HTML parsing
        return reviews
    
    def parse_generic(self) -> List[Dict[str, Any]]:
        """Generic parsing for unknown formats"""
        # Fallback parsing method
        reviews = []
        return reviews
    
    def is_review_text(self, text: str) -> bool:
        """Check if text is likely a review"""
        if len(text) < 10:
            return False
        
        # Filter out URLs and common non-review patterns
        exclude_patterns = [
            'http', 'www.', 'google.com', 'googleusercontent',
            'maps.google', 'gstatic.com', 'googleg_48dp'
        ]
        
        text_lower = text.lower()
        for pattern in exclude_patterns:
            if pattern in text_lower:
                return False
        
        return True
    
    def clean_text(self, text: str) -> str:
        """Clean and decode text"""
        try:
            # Decode unicode escapes
            cleaned = text.encode().decode('unicode_escape')
        except:
            cleaned = text
        
        # Basic cleaning
        cleaned = re.sub(r'\\u[0-9a-fA-F]{4}', '', cleaned)  # Remove unicode escapes
        cleaned = re.sub(r'\\[nr]', ' ', cleaned)  # Replace newlines
        cleaned = cleaned.strip()
        
        return cleaned
    
    def is_valid_review(self, review: Dict[str, Any]) -> bool:
        """Check if extracted review is valid"""
        has_user = bool(review.get('user_info', {}).get('name'))
        has_text = bool(review.get('review_text'))
        has_rating = review.get('rating') is not None
        has_date = bool(review.get('date_info'))
        
        return has_user or has_text or has_rating or has_date
    
    def calculate_confidence(self, review: Dict[str, Any]) -> float:
        """Calculate extraction confidence"""
        score = 0.0
        
        # User info
        if review.get('user_info', {}).get('name'):
            score += 0.2
        if review.get('user_info', {}).get('user_id'):
            score += 0.1
        
        # Content
        if review.get('review_text'):
            score += 0.3
            if len(review['review_text']) > 50:
                score += 0.1
        
        # Rating
        if review.get('rating') is not None:
            score += 0.2
        
        # Date
        if review.get('date_info'):
            score += 0.1
        
        return min(score, 1.0)
    
    def save_reviews(self, output_file: str) -> List[Dict[str, Any]]:
        """Save parsed reviews with comprehensive metadata"""
        reviews = self.parse_reviews()
        
        # Add confidence scores
        for review in reviews:
            review['confidence'] = self.calculate_confidence(review)
        
        # Calculate statistics
        ratings = [r['rating'] for r in reviews if r.get('rating') is not None]
        avg_rating = sum(ratings) / len(ratings) if ratings else None
        
        metadata = {
            'total_reviews': len(reviews),
            'parser_type': self.parser_type,
            'extraction_timestamp': datetime.now().isoformat(),
            'statistics': {
                'average_rating': avg_rating,
                'rating_distribution': {str(i): ratings.count(i) for i in range(1, 6)},
                'reviews_with_text': len([r for r in reviews if r.get('review_text')]),
                'reviews_with_images': len([r for r in reviews if r.get('review_images')]),
                'reviews_with_owner_response': len([r for r in reviews if r.get('owner_response')]),
                'average_confidence': sum(r.get('confidence', 0) for r in reviews) / len(reviews) if reviews else 0
            }
        }
        
        output_data = {
            'metadata': metadata,
            'reviews': reviews
        }
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, indent=2, ensure_ascii=False)
        
        print(f"Saved {len(reviews)} reviews to {output_file}")
        print(f"Parser type: {self.parser_type}")
        if avg_rating:
            print(f"Average rating: {avg_rating:.1f}")
        print(f"Average confidence: {metadata['statistics']['average_confidence']:.2f}")
        
        return reviews


def parse_file(html_file: str, output_file: str = None) -> List[Dict[str, Any]]:
    """Universal file parser"""
    try:
        with open(html_file, 'r', encoding='utf-8') as f:
            content = f.read()
        
        parser = UniversalGoogleMapsParser(content)
        
        if output_file:
            return parser.save_reviews(output_file)
        else:
            return parser.parse_reviews()
            
    except Exception as e:
        print(f"Error parsing file: {e}")
        return []


def main():
    """CLI interface"""
    if len(sys.argv) < 2:
        print("Usage: python universal_maps_parser.py <html_file> [output_file]")
        sys.exit(1)
    
    html_file = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else None
    
    if not output_file:
        import os
        base_name = os.path.splitext(os.path.basename(html_file))[0]
        output_file = f"universal_reviews_{base_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    
    reviews = parse_file(html_file, output_file)
    
    if reviews:
        print(f"\nExtracted {len(reviews)} reviews")
        
        # Show sample
        sample = reviews[0]
        user_name = sample.get('user_info', {}).get('name', 'Unknown')
        rating = sample.get('rating', 'N/A')
        text_preview = sample.get('review_text', 'No text')[:100]
        confidence = sample.get('confidence', 0)
        
        print(f"\nSample review:")
        print(f"User: {user_name}")
        print(f"Rating: {rating}")
        print(f"Text: {text_preview}...")
        print(f"Confidence: {confidence:.2f}")


if __name__ == "__main__":
    main()
