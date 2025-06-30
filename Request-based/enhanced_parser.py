import json
import re
import csv
from datetime import datetime
from typing import List, Dict, Any, Optional

class EnhancedGoogleMapsParser:
    def __init__(self, html_file_path: str):
        self.html_file_path = html_file_path
        self.raw_data = ""
        self.parsed_data = None
        self.reviews = []
        self.business_info = {}
        self.tokens = []
        self.review_details = []
        
    def load_html(self):
        """Load the HTML response file"""
        try:
            with open(self.html_file_path, 'r', encoding='utf-8') as file:
                self.raw_data = file.read()
            print(f"Successfully loaded HTML file: {self.html_file_path}")
        except Exception as e:
            print(f"Error loading HTML file: {e}")
            
    def extract_all_tokens(self):
        """Extract comprehensive set of tokens from Google Maps response"""
        print("Extracting tokens...")
        
        # Multiple token patterns for different types of Google tokens
        token_patterns = [
            # CIHM tokens (Content ID Hash Map)
            r'CIHM[A-Za-z0-9]{15,}',
            
            # Ch tokens (likely Content Hash)
            r'Ch[A-Za-z0-9]{15,}',
            
            # CAE tokens (Content Access Entry)
            r'CAE[A-Za-z0-9]{10,}',
            
            # 0ah tokens (likely internal reference)
            r'0ah[A-Za-z0-9_\-]{15,}',
            
            # Qd tokens (Query Data)
            r'Qd[A-Za-z0-9_\-]{15,}',
            
            # Base64-like tokens
            r'\b[A-Za-z0-9_\-]{20,}\b',
            
            # Google's internal identifiers
            r'"[A-Z][A-Za-z0-9_\-]{15,}"',
            
            # ugcs references
            r'UGCS_REFERENCE[^"]*',
            
            # ved parameters
            r'ved=[A-Za-z0-9_\-]+',
            
            # usg parameters  
            r'usg=[A-Za-z0-9_\-]+',
            
            # fid parameters
            r'fid=[A-Za-z0-9:x_\-]+',
            
            # Place IDs
            r'0x[a-f0-9]+:0x[a-f0-9]+',
            
            # Long numeric IDs
            r'\b\d{15,}\b',
        ]
        
        all_tokens = set()
        token_types = {}
        
        for pattern_name, pattern in zip([
            'CIHM_tokens', 'Ch_tokens', 'CAE_tokens', '0ah_tokens', 
            'Qd_tokens', 'Base64_tokens', 'Quoted_tokens', 'UGCS_tokens',
            'ved_params', 'usg_params', 'fid_params', 'place_ids', 'numeric_ids'
        ], token_patterns):
            
            matches = re.findall(pattern, self.raw_data)
            token_types[pattern_name] = []
            
            for match in matches:
                # Clean the token
                clean_token = match.strip('"').replace('ved=', '').replace('usg=', '').replace('fid=', '')
                if len(clean_token) > 8:  # Only keep meaningful tokens
                    all_tokens.add(clean_token)
                    token_types[pattern_name].append(clean_token)
        
        self.tokens = sorted(list(all_tokens))
        
        # Print token statistics
        print(f"\n=== TOKEN EXTRACTION SUMMARY ===")
        for token_type, tokens in token_types.items():
            if tokens:
                print(f"{token_type}: {len(set(tokens))} unique tokens")
        
        print(f"Total unique tokens extracted: {len(self.tokens)}")
        return self.tokens, token_types
    
    def extract_structured_reviews(self):
        """Extract reviews with detailed information"""
        print("Extracting detailed review information...")
        
        # Parse the JSON structure first
        try:
            if self.raw_data.startswith(")]}'\n"):
                json_data = self.raw_data[5:]
            else:
                json_data = self.raw_data
            
            self.parsed_data = json.loads(json_data)
        except:
            print("Could not parse as JSON, using regex extraction")
        
        # Extract reviews using various methods
        self._extract_reviews_from_patterns()
        self._extract_reviewer_info()
        self._extract_business_details()
        
        print(f"Extracted {len(self.review_details)} detailed reviews")
    
    def _extract_reviews_from_patterns(self):
        """Extract reviews using regex patterns"""
        # Pattern for review text with ratings and metadata
        review_patterns = [
            # Quoted review text
            r'"([^"]{30,500})"',
            # Review text with common restaurant keywords
            r'"([^"]*(?:food|service|great|good|excellent|recommend|delicious|order|meal|restaurant|chicken|beef|shrimp|rice|noodles)[^"]*)"',
        ]
        
        found_reviews = set()
        
        for pattern in review_patterns:
            matches = re.findall(pattern, self.raw_data, re.IGNORECASE)
            for match in matches:
                if (len(match) > 25 and 
                    any(word in match.lower() for word in [
                        'food', 'service', 'great', 'good', 'excellent', 'delicious', 
                        'recommend', 'order', 'meal', 'restaurant', 'chicken', 'beef',
                        'rice', 'noodles', 'fresh', 'tasty', 'price', 'staff'
                    ]) and
                    match not in found_reviews):
                    
                    found_reviews.add(match)
                    
                    # Try to find rating near this review
                    rating = self._find_rating_near_text(match)
                    
                    review_data = {
                        'review_text': match.strip(),
                        'rating': rating,
                        'author': None,
                        'date': None,
                        'review_length': len(match),
                        'language': 'en'  # Assuming English
                    }
                    
                    self.review_details.append(review_data)
    
    def _find_rating_near_text(self, review_text):
        """Try to find rating associated with review text"""
        # Look for patterns like [5], [[5]], rating patterns near the review
        text_pos = self.raw_data.find(review_text)
        if text_pos != -1:
            # Check 500 characters before and after
            context = self.raw_data[max(0, text_pos-500):text_pos+len(review_text)+500]
            
            # Look for rating patterns
            rating_patterns = [
                r'\[(\d)\]',  # [5]
                r'\[\[(\d)\]\]',  # [[5]]
                r'"rating":\s*(\d)',  # "rating": 5
                r'rating.*?(\d)',  # rating followed by digit
            ]
            
            for pattern in rating_patterns:
                matches = re.findall(pattern, context)
                for match in matches:
                    rating = int(match)
                    if 1 <= rating <= 5:
                        return rating
        return None
    
    def _extract_reviewer_info(self):
        """Extract reviewer names and metadata"""
        # Patterns for reviewer names and info
        reviewer_patterns = [
            r'"([A-Z][a-z]+ [A-Z][a-z]+)"',  # "First Last"
            r'"([A-Z][a-z]+)"',  # "Name"
        ]
        
        # Look for Local Guide indicators
        local_guide_pattern = r'Local Guide[^"]*(\d+) reviews'
        
        reviewers = []
        for pattern in reviewer_patterns:
            matches = re.findall(pattern, self.raw_data)
            for match in matches:
                if len(match) > 2 and len(match) < 50:  # Reasonable name length
                    reviewers.append(match)
        
        # Try to associate reviewers with reviews
        for i, review in enumerate(self.review_details):
            if i < len(reviewers):
                review['author'] = reviewers[i]
    
    def _extract_business_details(self):
        """Extract comprehensive business information"""
        # Business name
        name_match = re.search(r'"Kim\'s Island"', self.raw_data)
        if name_match:
            self.business_info['name'] = "Kim's Island"
        
        # Address
        address_match = re.search(r'"175 Main St[^"]*Staten Island[^"]*"', self.raw_data)
        if address_match:
            self.business_info['address'] = address_match.group().strip('"')
        
        # Phone number
        phone_match = re.search(r'(\+1 718-356-5168|\(718\) 356-5168)', self.raw_data)
        if phone_match:
            self.business_info['phone'] = phone_match.group()
        
        # Website
        website_match = re.search(r'kimsislandsi\.com', self.raw_data)
        if website_match:
            self.business_info['website'] = 'http://kimsislandsi.com/'
        
        # Rating
        rating_match = re.search(r'"rating":\s*([0-9.]+)', self.raw_data)
        if rating_match:
            self.business_info['rating'] = float(rating_match.group(1))
        
        # Review count
        review_count_match = re.search(r'(\d+) reviews', self.raw_data)
        if review_count_match:
            self.business_info['review_count'] = int(review_count_match.group(1))
        
        # Business type
        self.business_info['business_type'] = 'Chinese Restaurant'
        self.business_info['delivery_available'] = True
        
        # Extract price range
        price_match = re.search(r'\$10[–\-]20', self.raw_data)
        if price_match:
            self.business_info['price_range'] = '$10–20'
    
    def save_comprehensive_output(self):
        """Save all extracted data to multiple files"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # 1. Save tokens to file
        token_file = f"google_maps_tokens_{timestamp}.txt"
        with open(token_file, 'w', encoding='utf-8') as f:
            f.write("# Google Maps Tokens Extract\n")
            f.write(f"# Extracted on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"# Total tokens: {len(self.tokens)}\n")
            f.write(f"# Source: {self.html_file_path}\n\n")
            
            for token in self.tokens:
                f.write(f"{token}\n")
        
        # 2. Save detailed reviews to CSV
        reviews_file = f"google_maps_reviews_{timestamp}.csv"
        with open(reviews_file, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['review_text', 'rating', 'author', 'date', 'review_length', 'language']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            writer.writeheader()
            for review in self.review_details:
                writer.writerow(review)
        
        # 3. Save business info to JSON
        business_file = f"google_maps_business_{timestamp}.json"
        with open(business_file, 'w', encoding='utf-8') as f:
            json.dump(self.business_info, f, indent=2, ensure_ascii=False)
        
        # 4. Save summary report
        summary_file = f"google_maps_summary_{timestamp}.txt"
        with open(summary_file, 'w', encoding='utf-8') as f:
            f.write("GOOGLE MAPS DATA EXTRACTION SUMMARY\n")
            f.write("=" * 50 + "\n\n")
            f.write(f"Extraction Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Source File: {self.html_file_path}\n\n")
            
            f.write("BUSINESS INFORMATION:\n")
            f.write("-" * 25 + "\n")
            for key, value in self.business_info.items():
                f.write(f"{key.title()}: {value}\n")
            
            f.write(f"\nEXTRACTED DATA COUNTS:\n")
            f.write("-" * 25 + "\n")
            f.write(f"Total Tokens: {len(self.tokens)}\n")
            f.write(f"Total Reviews: {len(self.review_details)}\n")
            
            f.write(f"\nSAMPLE REVIEWS:\n")
            f.write("-" * 25 + "\n")
            for i, review in enumerate(self.review_details[:5]):
                f.write(f"{i+1}. {review['review_text'][:100]}...\n")
                if review['rating']:
                    f.write(f"   Rating: {review['rating']}/5\n")
                f.write(f"   Length: {review['review_length']} characters\n\n")
            
            f.write(f"\nTOKEN SAMPLES:\n")
            f.write("-" * 25 + "\n")
            for i, token in enumerate(self.tokens[:10]):
                f.write(f"{i+1}. {token}\n")
            
            if len(self.tokens) > 10:
                f.write(f"... and {len(self.tokens) - 10} more tokens\n")
        
        return {
            'tokens_file': token_file,
            'reviews_file': reviews_file,
            'business_file': business_file,
            'summary_file': summary_file
        }
    
    def run_complete_analysis(self):
        """Run the complete analysis pipeline"""
        print("🚀 Starting Google Maps Complete Analysis")
        print("=" * 50)
        
        # Step 1: Load data
        self.load_html()
        if not self.raw_data:
            print("❌ Failed to load data")
            return
        
        # Step 2: Extract tokens
        print("\n📋 Step 1: Extracting tokens...")
        tokens, token_types = self.extract_all_tokens()
        
        # Step 3: Extract reviews and business info
        print("\n📝 Step 2: Extracting reviews and business information...")
        self.extract_structured_reviews()
        
        # Step 4: Save all outputs
        print("\n💾 Step 3: Saving extracted data...")
        files = self.save_comprehensive_output()
        
        # Step 5: Print final summary
        print("\n✅ EXTRACTION COMPLETE!")
        print("=" * 50)
        print(f"🏢 Business: {self.business_info.get('name', 'Unknown')}")
        print(f"📍 Address: {self.business_info.get('address', 'Unknown')}")
        print(f"⭐ Rating: {self.business_info.get('rating', 'Unknown')}")
        print(f"🔢 Tokens Extracted: {len(self.tokens)}")
        print(f"💬 Reviews Extracted: {len(self.review_details)}")
        
        print(f"\n📁 Files Created:")
        for file_type, filename in files.items():
            print(f"   • {file_type.replace('_', ' ').title()}: {filename}")
        
        print(f"\n📊 Top 5 Sample Reviews:")
        for i, review in enumerate(self.review_details[:5]):
            rating_str = f" ({review['rating']}⭐)" if review['rating'] else ""
            print(f"   {i+1}. {review['review_text'][:80]}...{rating_str}")

def main():
    # Initialize the enhanced parser
    parser = EnhancedGoogleMapsParser("response.html")
    
    # Run complete analysis
    parser.run_complete_analysis()

if __name__ == "__main__":
    main()
