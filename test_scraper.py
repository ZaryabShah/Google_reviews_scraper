#!/usr/bin/env python3
"""
Quick test of the enhanced scraper with source and timing fields
"""

import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

from dual_async_scraper_v3 import DualAsyncGoogleMapsReviewScraper

def test_scraper_with_sample_data():
    """Test the scraper with the response.html file"""
    print("Testing Enhanced Scraper with Source and Timing Fields")
    print("=" * 60)
    
    # Create scraper instance (using a dummy place_id for testing)
    scraper = DualAsyncGoogleMapsReviewScraper("dummy_place_id")
    
    # Read the sample response
    try:
        with open('response.html', 'r', encoding='utf-8') as f:
            html_content = f.read()
    except FileNotFoundError:
        print("Error: response.html not found!")
        return
    
    # Test the parsing
    try:
        reviews = scraper.parse_reviews_from_response(html_content, "TEST")
        
        print(f"Successfully parsed {len(reviews)} reviews")
        print()
        
        # Show detailed information for first few reviews
        for i, review in enumerate(reviews[:3]):
            print(f"Review {i+1}:")
            print(f"  Reviewer: {review.get('reviewerName', 'Unknown')}")
            print(f"  Rating: {review.get('stars', 'N/A')}")
            print(f"  Source: {review.get('reviewSource', 'Unknown')}")
            print(f"  Source Type: {review.get('reviewSourceType', 'Unknown')}")
            print(f"  Timing: {review.get('reviewTiming', 'Unknown')}")
            print(f"  Time Ago: {review.get('timeAgo', 'Unknown')}")
            print(f"  Text Preview: {review.get('text', 'No text')[:100]}...")
            print()
        
        # Summary of all reviews
        sources = [r.get('reviewSource', 'Unknown') for r in reviews]
        timings = [r.get('reviewTiming', 'Unknown') for r in reviews if r.get('reviewTiming') != 'Unknown']
        
        print("SCRAPER TEST SUMMARY:")
        print(f"Total reviews parsed: {len(reviews)}")
        print(f"Sources found: Google={sources.count('Google')}, Tripadvisor={sources.count('Tripadvisor')}, Other={len(sources) - sources.count('Google') - sources.count('Tripadvisor')}")
        print(f"Reviews with timing: {len(timings)}/{len(reviews)}")
        print(f"Sample timings: {timings[:5]}")
        
        # Check new fields are present
        has_source = sum(1 for r in reviews if r.get('reviewSource') and r.get('reviewSource') != 'Unknown')
        has_timing = sum(1 for r in reviews if r.get('reviewTiming') and r.get('reviewTiming') != 'Unknown')
        
        print(f"Reviews with source field: {has_source}/{len(reviews)} ({has_source/len(reviews)*100:.1f}%)")
        print(f"Reviews with timing field: {has_timing}/{len(reviews)} ({has_timing/len(reviews)*100:.1f}%)")
        
        return True
        
    except Exception as e:
        print(f"Error testing scraper: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_scraper_with_sample_data()
    if success:
        print("\n✅ Test completed successfully!")
    else:
        print("\n❌ Test failed!")
