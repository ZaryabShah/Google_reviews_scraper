import asyncio
import json
from typing import List, Optional
from playwright.async_api import async_playwright, Page, ElementHandle
from .model import GoogleMapReviewData
from dateparser import parse as parse_date  # make sure to install dateparser

class GoogleMapReviewsScraper:
    async def init_browser(self) -> tuple:
        """Initialize browser and page"""
        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.chromium.launch(headless=True)
        self.page = await self.browser.new_page()
        return self.playwright, self.browser, self.page

    async def extract_review_data(
        self,
        review_element: ElementHandle,
        place_id: str,
        cid: str,
        fid: str,
        total_score: float
    ) -> Optional[GoogleMapReviewData]:
        """Extract data from a single review element"""
        try:
            # Extract reviewer name
            name_element = await review_element.query_selector(".d4r55")
            reviewerName = await name_element.inner_text() if name_element else ""
            
            link_element = await review_element.query_selector("button[data-href]")
            reviewerUrl = await link_element.get_attribute("data-href") if link_element else ""
            reviewerId = reviewerUrl.split("/")[-1].split("?")[0] if reviewer_url else ""

            img_element = await review_element.query_selector(".NBa7we")
            reviewerPhotoUrl = await img_element.get_attribute("src") if img_element else ""

            # Extract rating (number of filled stars)
            rating = await review_element.query_selector_all(".hCCjke.google-symbols.NhBTye.elGi1d")
            stars = len(rating)

            # Extract review date text and convert to ISO (simplified)
            date_element = await review_element.query_selector(".rsqaWe")
            date_text = await date_element.inner_text() if date_element else ""
            parsed_date = parse_date(date_text)
            publishedAtDate = parsed_date.isoformat() if parsed_date else ""

            # Extract review text
            text_element = await review_element.query_selector(".wiI7pd")
            text = await text_element.inner_text() if text_element else ""

            # Click "More" button if it exists to expand review
            more_button = await review_element.query_selector("button.w8nwRe.kyuRq")
            if more_button:
                await more_button.click()
                await asyncio.sleep(1)
                text_element = await review_element.query_selector(".wiI7pd")
                text = await text_element.inner_text() if text_element else text

            # Extract photos
            reviewImageUrls = []
            photo_elements = await review_element.query_selector_all(".Tya61d")
            for photo in photo_elements:
                style = await photo.get_attribute("style")
                if style and "url(" in style:
                    url = style.split('url("')[1].split('")')[0]
                    reviewImageUrls.append(url)

            # Extract likes count
            likes_element = await review_element.query_selector(".pkWtMe")
            likesCount = await likes_element.inner_text() if likes_element else "0"

            # Get review ID and construct review URL (optional)
            reviewId = await review_element.get_attribute("data-review-id")
            reviewUrl = f"https://www.google.com/maps/reviews/data=!4m8!14m7!1m6!2m5!1s{reviewId}" if review_id else ""

            return GoogleMapReviewData(
                reviewerId=reviewerId,
                reviewerUrl=reviewerUrl,
                reviewerName=reviewerName,
                reviewerNumberOfReviews=0, #should be modified
                reviewerPhotoUrl=reviewerPhotoUrl,
                text=text,
                reviewImageUrls=reviewImageUrls,
                publishedAtDate=publishedAtDate, 
                lastEditedAtDate=publishedAtDate #should be modified
                likesCount=int(likesCount) if likesCount.isdigit() else 0
                reviewId=reviewId or "",
                reviewUrl=reviewUrl,
                stars=stars,
                placeId=place_id, #should be modified
                location={ #should be modified
                    "lat": 40,
                    "lng": 40
                },
                address="", #should be modified
                neighborhood="", #should be modified
                street="", #should be modified
                city="", #should be modified
                postalCode="", #should be modified
                categories=[ #should be modified
                    "a",
                    "b",
                    "c"
                ],
                title="", #should be modified
                totalScore=totalScore, #should be modified
                url="", #should be modified
                price=null, #should be modified
                cid=cid, #should be modified
                fid=fid, #should be modified
                scrapedAt=publishedAtDate #should be modified
            )

        except Exception as e:
            print(f"❌ Error extracting review data: {str(e)}")
            return None


    async def scroll_reviews(self, page: Page) -> bool:
        """Scroll the reviews panel and wait for new content"""
        try:
            # Get the scrollable reviews container
            reviews_container = await page.query_selector('div[tabindex="-1"].m6QErb')
            if not reviews_container:
                print("❌ Review container not found")
                return False
            
             # Measure current scroll height
            prev_height = await reviews_container.evaluate("(el) => el.scrollHeight")

            # Scroll down by a large amount to trigger lazy loading
            await reviews_container.evaluate("(el) => el.scrollBy(0, el.scrollHeight)")
            await asyncio.sleep(10)  # Wait for lazy-loaded content to render

            # Check if the scroll height has changed (i.e., new reviews loaded)
            new_height = await reviews_container.evaluate("(el) => el.scrollHeight")

            scrolled = new_height > prev_height
            print(f"Scrolled: {'✅' if scrolled else '❌'} — height: {prev_height} → {new_height}")
            return scrolled

        except Exception as e:
            print(f"Error during scrolling: {e}")
            return False


    async def scrape_reviews(self, url: str) -> List[GoogleMapReviewData]:
        """Scrape all reviews from a single URL"""
        playwright, browser, page = await self.init_browser()
        reviews = []
        processed_review_ids = set()

        try:
            clean_url = url.split("/@")[0]
            print(f"\nScraping reviews from: {clean_url}")
            await page.goto(url)

            # Get total review count
            more_reviews_btn = await page.query_selector('button[aria-label^="More reviews"]')
            if more_reviews_btn:
                aria_label = await more_reviews_btn.get_attribute("aria-label")
                import re
                match = re.search(r"\((\d+)\)", aria_label)
                total_review_count = int(match.group(1)) if match else None
            else:
                total_review_count = None

            # Click on More reviews button
            try:
                await page.click('button[aria-label^="More reviews"]', timeout=10000)
                print("Clicked on 'More reviews' button")
            except:
                print("Failed to find 'More reviews' button")
                return []

            # Try to sort by newest
            try:
                await page.click("button[data-value='Sort']", timeout=10000)
                await page.wait_for_selector('div[role="menu"]')
                await page.click('div[role="menuitemradio"][data-index="1"]')
                print("Sorted by newest")
            except:
                print("Failed to sort")

            # Wait for reviews to appear
            await page.wait_for_selector(".jftiEf", timeout=10000)

            # Scroll and collect reviews
            max_expected_reviews = total_review_count if total_review_count else 200
            print(f"Target: {max_expected_reviews} reviews")

            while len(processed_review_ids) < max_expected_reviews:
                review_elements = await page.query_selector_all(".jftiEf")

                for review in review_elements:
                    review_id = await review.get_attribute("data-review-id")
                    if review_id and review_id not in processed_review_ids:
                        # review_data = await self.extract_review_data(review)
                        review_data = await self.extract_review_data(
                            review,
                            place_id='xxx',
                            cid='xxx',
                            fid='xxx',
                            total_score=4.9
                        )

                        if review_data:
                            reviews.append(review_data)
                            processed_review_ids.add(review_id)
                            print(f"Scraped review by {review_data.reviewerName} ({len(reviews)} total)")

                if len(processed_review_ids) >= max_expected_reviews:
                    print("✅ Reached expected review count — stopping")
                    break

                new_content = await self.scroll_reviews(page)
                if not new_content:
                    print("⚠️ No more content to scroll — stopping")
                    break

                await asyncio.sleep(2)

        except Exception as e:
            print(f"Error during scraping: {e}")
        finally:
            await browser.close()
            await playwright.stop()

        return reviews

    async def scrape_multiple_places(self, urls: List[str]) -> List[GoogleMapReviewData]:
        """Scrape reviews from multiple URLs"""
        all_reviews = []
        for url in urls:
            reviews = await self.scrape_reviews(url)
            all_reviews.extend(reviews)
            print(f"Completed scraping {len(reviews)} reviews from current URL")
        return all_reviews

    def save_reviews(self, reviews: List[GoogleMapReviewData], filename: str = None):
        """Save reviews to a JSON file"""
        if filename is None:
            filename = f"free_scraper_output.json"

        with open(filename, "w", encoding="utf-8") as f:
            json.dump(
                [self._review_to_dict(review) for review in reviews],
                f,
                indent=4,
                ensure_ascii=False
            )
        print(f"\nSaved {len(reviews)} reviews to {filename}")

    def _review_to_dict(self, review: GoogleMapReviewData) -> dict:
        return {
            "reviewerId": review.reviewerId,
            "reviewerUrl": review.reviewerUrl,
            "reviewerName": review.reviewerName,
            "reviewId": review.reviewId,
            "reviewUrl": review.reviewUrl,
            "publishedAtDate": review.publishedAtDate,
            "placeId": review.placeId,
            "cid": review.cid,
            "fid": review.fid,
            "totalScore": review.totalScore,
            "text": review.text,
            "photos": review.photos,
            "likes_count": review.likes_count  # you can rename this to likesCount if needed
        }
