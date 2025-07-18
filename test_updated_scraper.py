#!/usr/bin/env python3
"""
Test script for the updated scraper that:
1. Only saves reviews (no metadata summary)
2. Doesn't save tokens file
"""

import asyncio
import json
import os
from dual_async_scraper_v3 import DualAsyncGoogleMapsReviewScraper

async def test_updated_scraper():
    """Test the updated scraper with simplified output"""
    
    # Test URL - using a small business for quick testing
    test_url = "https://maps.google.com/maps?cid=0xaa8fe344e1e346b3"
    
    # Extract place_id from URL for scraper initialization
    place_id = "aa8fe344e1e346b3"  # Extracted from the URL
    
    scraper = DualAsyncGoogleMapsReviewScraper(place_id)
    
    try:
        print("Testing updated scraper with simplified output...")
        print("=" * 60)
        
        # Run scraper with minimal pages for testing
        results = await scraper.scrape_reviews_dual_direction(
            google_maps_url=test_url,
            max_pages_per_direction=1,  # Just 1 page for testing
            delay_between_requests=2
        )
        
        if results and len(results) > 0:
            print(f"\nExtracted {len(results)} reviews")
            
            # Check the structure of the output file
            output_file = scraper.output_file
            print(f"\nChecking output file: {output_file}")
            
            if os.path.exists(output_file):
                with open(output_file, 'r', encoding='utf-8') as f:
                    file_content = json.load(f)
                
                print("\nFile structure validation:")
                print("-" * 30)
                
                # Check if it's just an array (no metadata wrapper)
                if isinstance(file_content, list):
                    print("✅ Output is a simple array of reviews (no metadata)")
                    print(f"✅ Contains {len(file_content)} reviews")
                    
                    # Validate first review structure
                    if file_content:
                        first_review = file_content[0]
                        expected_fields = {'reviewerName', 'rating', 'published_at', 'timeAgo', 'source', 'text'}
                        actual_fields = set(first_review.keys())
                        
                        if actual_fields == expected_fields:
                            print("✅ Reviews have exactly the 6 expected fields")
                        else:
                            print(f"❌ Review fields mismatch:")
                            print(f"   Expected: {expected_fields}")
                            print(f"   Actual: {actual_fields}")
                            print(f"   Missing: {expected_fields - actual_fields}")
                            print(f"   Extra: {actual_fields - expected_fields}")
                else:
                    print("❌ Output still contains metadata wrapper")
                    print(f"   File content type: {type(file_content)}")
                    if isinstance(file_content, dict):
                        print(f"   Keys: {list(file_content.keys())}")
            else:
                print(f"❌ Output file not found: {output_file}")
            
            # Check if tokens file was created (it shouldn't be)
            script_dir = os.path.dirname(output_file)
            tokens_files = [f for f in os.listdir(script_dir) if f.startswith('dual_tokens_') and f.endswith('.json')]
            
            if not tokens_files:
                print("✅ No tokens file created (as expected)")
            else:
                print(f"❌ Tokens file(s) still being created: {tokens_files}")
            
            print("\nSample reviews (first 2):")
            print("=" * 40)
            
            for i, review in enumerate(results[:2]):
                print(f"\nReview {i+1}:")
                print(f"  reviewerName: {review.get('reviewerName', 'N/A')}")
                print(f"  rating: {review.get('rating', 'N/A')}")
                print(f"  published_at: {review.get('published_at', 'N/A')}")
                print(f"  timeAgo: {review.get('timeAgo', 'N/A')}")
                print(f"  source: {review.get('source', 'N/A')}")
                print(f"  text: {review.get('text', 'N/A')[:80]}...")
            
        else:
            print("❌ No reviews extracted")
            
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(test_updated_scraper())
