import json
import re
from urllib.parse import unquote
from datetime import datetime

def extract_caesy_tokens(html_content):
    """Extract all tokens starting with CAESY0NBRV"""
    # Find all tokens that start with CAESY0NBRV
    caesy_tokens = re.findall(r'CAESY0[A-Za-z0-9_\-+=]{10,}', html_content)
    
    # Remove duplicates while preserving order
    unique_tokens = []
    seen = set()
    for token in caesy_tokens:
        if token not in seen:
            unique_tokens.append(token)
            seen.add(token)
    
    return unique_tokens

def extract_business_data(html_content):
    """Extract business/restaurant data"""
    business_data = {}
    
    try:
        # Extract business name
        name_match = re.search(r'"([^"]*Island[^"]*)"', html_content)
        if name_match:
            business_data['name'] = name_match.group(1)
        
        # Extract address
        address_match = re.search(r'"([\d]+\s+Main\s+St[^"]*Staten\s+Island[^"]*)"', html_content)
        if address_match:
            business_data['address'] = address_match.group(1)
        
        # Extract phone number
        phone_match = re.search(r'"\+1\s+(718-[\d-]+)"', html_content)
        if phone_match:
            business_data['phone'] = f"+1 {phone_match.group(1)}"
        
        # Extract website
        website_match = re.search(r'"(http://[^"]*kimsisland[^"]*)"', html_content)
        if website_match:
            business_data['website'] = website_match.group(1)
        
        # Extract rating and review count
        rating_match = re.search(r'null,(\d+\.\d+),(\d+)', html_content)
        if rating_match:
            business_data['rating'] = float(rating_match.group(1))
            business_data['review_count'] = int(rating_match.group(2))
        
        # Extract business type
        type_match = re.search(r'"(Chinese\s+restaurant)"', html_content)
        if type_match:
            business_data['business_type'] = type_match.group(1)
        
        # Extract price range
        price_match = re.search(r'"\$(\d+)–(\d+)"', html_content)
        if price_match:
            business_data['price_range'] = f"${price_match.group(1)}–{price_match.group(2)}"
        
        # Extract coordinates
        coord_match = re.search(r'null,null,(40\.\d+),(-74\.\d+)', html_content)
        if coord_match:
            business_data['coordinates'] = {
                'latitude': float(coord_match.group(1)),
                'longitude': float(coord_match.group(2))
            }
        
        # Extract hours information
        hours_match = re.search(r'"Closed\s+⋅\s+Opens\s+(\d+\s+AM\s+\w+)"', html_content)
        if hours_match:
            business_data['current_status'] = f"Closed ⋅ Opens {hours_match.group(1)}"
        
        # Extract delivery availability
        if 'delivery' in html_content.lower():
            business_data['delivery_available'] = True
        else:
            business_data['delivery_available'] = False
            
    except Exception as e:
        print(f"Error extracting business data: {e}")
    
    return business_data

def extract_reviews_data(html_content):
    """Extract Google reviews with user details"""
    reviews = []
    
    try:
        # Find review patterns in the HTML
        # Look for review structures with user names, ratings, and text
        review_patterns = re.findall(
            r'"([^"]*(?:Cohen|Chan|Stewart|Castellano|Joan|Catherine|Valerie)[^"]*)"[^"]*"([^"]*lh3\.googleusercontent\.com[^"]*)"[^"]*"([^"]*maps/contrib/[^"]*)"[^"]*"(\d+)"[^"]*null,(\d+),(\d+)[^"]*\[\[(\d)\][^"]*"([^"]*(?:years?|months?|days?)\s+ago)"[^"]*"([^"]*)"', 
            html_content
        )
        
        # Also look for review text patterns
        review_texts = re.findall(
            r'"((?:Great|Excellent|Really|I\s+been|This\s+place|After\s+moving)[^"]{20,200})"', 
            html_content
        )
        
        # Extract review data more systematically
        user_names = re.findall(r'"(Katelyn Cohen|Richard Chan|L C|Stacie Stewart|Rocco Castellano|Joan|Catherine Mahon|Valerie Tagliavia)"', html_content)
        
        # Find rating patterns
        ratings = re.findall(r'\[\[(\d)\]', html_content)
        
        # Find time patterns  
        times = re.findall(r'"(\d+\s+(?:years?|months?|days?)\s+ago)"', html_content)
        
        # Combine the data
        for i, name in enumerate(user_names):
            review = {
                'reviewer_name': name,
                'rating': int(ratings[i]) if i < len(ratings) else None,
                'time_ago': times[i] if i < len(times) else None,
                'review_text': review_texts[i] if i < len(review_texts) else None,
                'reviewer_id': None,
                'profile_image': None
            }
            
            # Try to extract more specific data for this reviewer
            name_pattern = re.escape(name)
            reviewer_data = re.search(
                rf'"{name_pattern}"[^"]*"(https://lh3\.googleusercontent\.com[^"]*)"[^"]*"([^"]*maps/contrib/(\d+)[^"]*)"[^"]*"(\d+)"',
                html_content
            )
            
            if reviewer_data:
                review['profile_image'] = reviewer_data.group(1)
                review['profile_url'] = reviewer_data.group(2)
                review['reviewer_id'] = reviewer_data.group(3)
                review['review_count'] = reviewer_data.group(4)
            
            reviews.append(review)
        
        # Remove duplicates based on reviewer name
        unique_reviews = []
        seen_names = set()
        for review in reviews:
            if review['reviewer_name'] not in seen_names:
                unique_reviews.append(review)
                seen_names.add(review['reviewer_name'])
        
        reviews = unique_reviews
        
    except Exception as e:
        print(f"Error extracting reviews: {e}")
    
    return reviews

def main():
    # Read the HTML file
    try:
        with open('response.html', 'r', encoding='utf-8') as file:
            html_content = file.read()
    except FileNotFoundError:
        print("Error: response.html file not found!")
        return
    except Exception as e:
        print(f"Error reading HTML file: {e}")
        return
    
    # Extract CAESY tokens
    print("Extracting CAESY0NBRV tokens...")
    caesy_tokens = extract_caesy_tokens(html_content)
    
    # Extract business data
    print("Extracting business data...")
    business_data = extract_business_data(html_content)
    
    # Extract reviews
    print("Extracting reviews data...")
    reviews_data = extract_reviews_data(html_content)
    
    # Prepare tokens data
    tokens_data = {
        'extraction_timestamp': datetime.now().isoformat(),
        'total_tokens_found': len(caesy_tokens),
        'tokens': caesy_tokens
    }
    
    # Prepare combined business and reviews data
    combined_data = {
        'extraction_timestamp': datetime.now().isoformat(),
        'business_info': business_data,
        'reviews': reviews_data,
        'total_reviews_found': len(reviews_data)
    }
    
    # Generate timestamp for file names
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Save tokens to JSON file
    tokens_filename = f'caesy_tokens_{timestamp}.json'
    try:
        with open(tokens_filename, 'w', encoding='utf-8') as file:
            json.dump(tokens_data, file, indent=2, ensure_ascii=False)
        print(f"✅ Tokens saved to: {tokens_filename}")
        print(f"   Total CAESY0NBRV tokens found: {len(caesy_tokens)}")
    except Exception as e:
        print(f"Error saving tokens: {e}")
    
    # Save business and reviews data to JSON file
    data_filename = f'business_reviews_{timestamp}.json'
    try:
        with open(data_filename, 'w', encoding='utf-8') as file:
            json.dump(combined_data, file, indent=2, ensure_ascii=False)
        print(f"✅ Business and reviews data saved to: {data_filename}")
        print(f"   Business data fields: {list(business_data.keys())}")
        print(f"   Total reviews found: {len(reviews_data)}")
    except Exception as e:
        print(f"Error saving business data: {e}")
    
    # Print summary
    print("\n=== EXTRACTION SUMMARY ===")
    print(f"CAESY0NBRV Tokens: {len(caesy_tokens)}")
    print(f"Business Name: {business_data.get('name', 'N/A')}")
    print(f"Business Rating: {business_data.get('rating', 'N/A')}")
    print(f"Total Reviews: {len(reviews_data)}")
    print(f"Review Count from Business: {business_data.get('review_count', 'N/A')}")
    
    if reviews_data:
        print("\nSample Review:")
        sample_review = reviews_data[0]
        print(f"  Reviewer: {sample_review.get('reviewer_name', 'N/A')}")
        print(f"  Rating: {sample_review.get('rating', 'N/A')}")
        print(f"  Time: {sample_review.get('time_ago', 'N/A')}")
        print(f"  Text: {sample_review.get('review_text', 'N/A')[:100]}...")
    
    if caesy_tokens:
        print(f"\nSample CAESY Token: {caesy_tokens[0]}")

if __name__ == "__main__":
    main()
