#!/usr/bin/env python3
"""
Test script for simplified scraper with only 6 fields:
- reviewerName
- rating
- published_at
- timeAgo
- source
- text
"""

import asyncio
import json
from dual_async_scraper_v3 import DualAsyncGoogleMapsReviewScraper

async def test_simplified_scraper():
    """Test the simplified scraper with only 6 fields"""
    
    # Test URL - using a small business for quick testing
    test_url = "https://maps.google.com/maps?cid=0xaa8fe344e1e346b3"
    
    # Extract place_id from URL for scraper initialization
    place_id = "aa8fe344e1e346b3"  # Extracted from the URL
    
    scraper = DualAsyncGoogleMapsReviewScraper(place_id)
    
    try:
        print("Testing simplified scraper with 6 fields only...")
        print("=" * 60)
        
        # Run scraper with minimal pages for testing
        results = await scraper.scrape_reviews_dual_direction(
            google_maps_url=test_url,
            max_pages_per_direction=1,  # Just 1 page for testing
            delay_between_requests=2
        )
        
        if results and 'reviews' in results and results['reviews']:
            print(f"\nExtracted {len(results['reviews'])} reviews")
            print("\nSample reviews (showing first 3):")
            print("=" * 60)
            
            for i, review in enumerate(results['reviews'][:3]):
                print(f"\nReview {i+1}:")
                print(f"  reviewerName: {review.get('reviewerName', 'N/A')}")
                print(f"  rating: {review.get('rating', 'N/A')}")
                print(f"  published_at: {review.get('published_at', 'N/A')}")
                print(f"  timeAgo: {review.get('timeAgo', 'N/A')}")
                print(f"  source: {review.get('source', 'N/A')}")
                print(f"  text: {review.get('text', 'N/A')[:100]}...")
                
                # Check for any unexpected fields
                expected_fields = {'reviewerName', 'rating', 'published_at', 'timeAgo', 'source', 'text'}
                actual_fields = set(review.keys())
                unexpected_fields = actual_fields - expected_fields
                
                if unexpected_fields:
                    print(f"  ⚠️  Unexpected fields found: {unexpected_fields}")
                else:
                    print(f"  ✅ Only expected fields present")
            
            print("\n" + "=" * 60)
            print("VALIDATION SUMMARY:")
            print("=" * 60)
            
            # Validate all reviews have only the 6 expected fields
            expected_fields = {'reviewerName', 'rating', 'published_at', 'timeAgo', 'source', 'text'}
            all_valid = True
            
            for i, review in enumerate(results['reviews']):
                actual_fields = set(review.keys())
                if actual_fields != expected_fields:
                    print(f"❌ Review {i+1} has incorrect fields:")
                    print(f"   Expected: {expected_fields}")
                    print(f"   Actual: {actual_fields}")
                    print(f"   Missing: {expected_fields - actual_fields}")
                    print(f"   Extra: {actual_fields - expected_fields}")
                    all_valid = False
                    break
            
            if all_valid:
                print(f"✅ All {len(results['reviews'])} reviews have exactly the 6 expected fields!")
                print(f"✅ Fields: {sorted(expected_fields)}")
            
            # Check data quality
            print("\nDATA QUALITY CHECK:")
            print("-" * 30)
            
            reviews_with_names = sum(1 for r in results['reviews'] if r.get('reviewerName'))
            reviews_with_ratings = sum(1 for r in results['reviews'] if r.get('rating') is not None)
            reviews_with_text = sum(1 for r in results['reviews'] if r.get('text'))
            reviews_with_source = sum(1 for r in results['reviews'] if r.get('source'))
            reviews_with_time = sum(1 for r in results['reviews'] if r.get('timeAgo'))
            
            total = len(results['reviews'])
            print(f"Reviews with reviewerName: {reviews_with_names}/{total} ({reviews_with_names/total*100:.1f}%)")
            print(f"Reviews with rating: {reviews_with_ratings}/{total} ({reviews_with_ratings/total*100:.1f}%)")
            print(f"Reviews with text: {reviews_with_text}/{total} ({reviews_with_text/total*100:.1f}%)")
            print(f"Reviews with source: {reviews_with_source}/{total} ({reviews_with_source/total*100:.1f}%)")
            print(f"Reviews with timeAgo: {reviews_with_time}/{total} ({reviews_with_time/total*100:.1f}%)")
            
        else:
            print("❌ No reviews extracted")
            
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(test_simplified_scraper())
