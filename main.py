import asyncio
from google_map_review.main import GoogleMapReviewsScraper
from google_map_review.utils import generate_urls_from_place_ids

async def scrap_google_map_reviews(placeIDs):
    # List of URLs to scrape
    urls = generate_urls_from_place_ids(place_ids=placeIDs)

    scraper = GoogleMapReviewsScraper()
    
    # Scrape all reviews
    all_reviews = await scraper.scrape_multiple_places(urls)
    
    # Save to JSON file
    scraper.save_reviews(all_reviews)

    return all_reviews

if __name__ == "__main__":
    asyncio.run(scrap_google_map_reviews(['ChIJJQz5EZzKw4kRCZ95UajbyGw']))
