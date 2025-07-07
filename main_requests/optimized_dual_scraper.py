"""
Optimized Google Maps Review Scraper
Addresses key performance bottlenecks:
1. JSON-based parsing instead of regex on full HTML
2. Producer-Consumer pattern for async pipeline
3. Minimal token storage
4. orjson for fast JSON processing
5. Typed dataclasses for memory efficiency
"""

import asyncio
import aiohttp
import json
import re
from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Set, List, Dict, Any, Optional
import time
import os
from urllib.parse import unquote

# Try to use orjson for better performance, fallback to standard json
try:
    import orjson
    def json_loads(data):
        return orjson.loads(data)
    def json_dumps(data):
        return orjson.dumps(data).decode()
except ImportError:
    print("Warning: orjson not available, using standard json (slower)")
    def json_loads(data):
        return json.loads(data)
    def json_dumps(data):
        return json.dumps(data)

@dataclass(slots=True)
class Review:
    """Optimized review data structure with slots for memory efficiency"""
    reviewId: str
    reviewerId: str
    reviewerName: str
    stars: int
    text: str
    language: str
    publishedAtDate: str
    likesCount: Optional[int] = 0
    ownerResponse: Optional[str] = None
    images: List[str] = field(default_factory=list)
    reviewerPhotoUrl: str = ""
    reviewerNumberOfReviews: int = 0
    isLocalGuide: bool = False
    sortDirection: str = ""
    extractionConfidence: float = 1.0
    timeAgo: str = ""
    hasImages: bool = False
    hasOwnerResponse: bool = False

class OptimizedGoogleMapsReviewScraper:
    """Optimized scraper using producer-consumer pattern and JSON parsing"""
    
    def __init__(self, place_id: str, max_queue_size: int = 30, num_workers: int = 2):
        self.place_id = place_id.replace("0x", "") if place_id.startswith("0x") else place_id
        self.base_url = "https://www.google.com/maps/rpc/listugcposts"
        
        # Reusable headers dict (global optimization)
        self.headers = {
            "accept": "*/*",
            "accept-language": "en-US,en;q=0.9",
            "cache-control": "no-cache",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
        }
        
        # Producer-Consumer setup
        self.queue = asyncio.Queue(maxsize=max_queue_size)
        self.stop_event = asyncio.Event()
        self.num_workers = num_workers
        
        # Shared state
        self.all_reviews: List[Review] = []
        self.seen_reviewer_ids: Set[str] = set()
        self.duplicate_count = 0
        self.duplicate_threshold = 15  # Stop if more than 15 duplicates in one batch
        
        # Minimal token tracking (only next tokens)
        self.next_token_highest: Optional[str] = None
        self.next_token_lowest: Optional[str] = None
        self.used_tokens: Set[str] = set()
        
        # Stats
        self.stats = {
            'highest_rating': {'pages': 0, 'reviews': 0, 'duplicates': 0},
            'lowest_rating': {'pages': 0, 'reviews': 0, 'duplicates': 0}
        }
        
        # File setup
        script_dir = os.path.dirname(os.path.abspath(__file__))
        clean_place_id = self.place_id.replace(":", "_")
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        self.output_file = os.path.join(script_dir, f"optimized_reviews_{clean_place_id}_{timestamp}.json")

    def build_querystring(self, continuation_token: Optional[str] = None, sort_by_highest: bool = True) -> Dict[str, str]:
        """Build the querystring for the request"""
        sort_value = "1e3" if sort_by_highest else "1e4"
        
        if continuation_token:
            pb_value = f"!1m6!1s0x{self.place_id}!6m4!4m1!1e1!4m1!1e3!2m2!1i20!2s{continuation_token}!5m2!1sStliaIi6EPWA9u8PwLTBwAE!7e81!8m9!2b1!3b1!5b1!7b1!12m4!1b1!2b1!4m1!1e1!11m0!13m1!{sort_value}"
        else:
            pb_value = f"!1m6!1s0x{self.place_id}!6m4!4m1!1e1!4m1!1e3!2m2!1i20!2s!5m2!1sStliaIi6EPWA9u8PwLTBwAE!7e81!8m9!2b1!3b1!5b1!7b1!12m4!1b1!2b1!4m1!1e1!11m0!13m1!{sort_value}"
        
        return {
            "authuser": "0",
            "hl": "en",
            "pb": pb_value
        }

    def strip_rpc_prefix(self, response_text: str) -> str:
        """Strip the Google RPC prefix to get clean JSON"""
        # Google RPC responses start with )]}' followed by JSON
        if response_text.startswith(")]}'"):
            return response_text[4:]
        return response_text

    def datetime_from_microseconds(self, microseconds: Optional[int]) -> str:
        """Convert microsecond timestamp to ISO format"""
        if not microseconds:
            return datetime.now().isoformat()
        try:
            timestamp_seconds = int(microseconds) / 1000000
            return datetime.fromtimestamp(timestamp_seconds).isoformat()
        except (ValueError, TypeError):
            return datetime.now().isoformat()

    def safe_get_nested(self, data: Any, *indices) -> Any:
        """Safely get nested data with fallback to None"""
        try:
            current = data
            for index in indices:
                current = current[index]
            return current
        except (IndexError, KeyError, TypeError):
            return None

    def _find_user_meta(self, block: list) -> Optional[list]:
        """Walk the list tree until we find the sub-list that holds
           [name, profile_img, [profile_url], user_id, …]."""
        if isinstance(block, list):
            for item in block:
                if (isinstance(item, list) and len(item) >= 4
                    and isinstance(item[0], str)
                    and isinstance(item[1], str)
                    and item[1].startswith("https://lh3")):
                    return item
                if isinstance(item, list):
                    found = self._find_user_meta(item)
                    if found:
                        return found
        return None

    def _find_likes(self, block: list) -> int:
        """Return the first `[1, n]` we meet (n may be 0)."""
        if isinstance(block, list):
            for item in block:
                if isinstance(item, list) and len(item) == 2 and item[0] == 1:
                    return int(item[1])
                if isinstance(item, list):
                    n = self._find_likes(item)
                    if n is not None:
                        return n
        return 0

    def _long_strings(self, block, path=()):
        """Generator for long strings in nested structures (used by image extractor)"""
        if isinstance(block, list):
            for i, item in enumerate(block):
                yield from self._long_strings(item, path + (i,))
        elif isinstance(block, str) and len(block) > 40:
            yield block, path

    def fast_parse_review(self, review_data: List[Any], direction: str) -> Optional[Review]:
        """Fast JSON-based review parsing using corrected stable index map"""
        try:
            meta = review_data[0]  # main payload

            # ------- rating ---------------------------------------------------------
            stars = self.safe_get_nested(meta, 2, 0, 0)
            if stars is None:
                return None  # malformed row

            # ------- text & language ------------------------------------------------
            text_bucket_idx, text, language = None, "", "en"
            for idx in range(len(meta[2]) - 1, -1, -1):  # walk backwards
                bucket = meta[2][idx]
                if (isinstance(bucket, list) and bucket
                    and isinstance(bucket[0], list)
                    and bucket[0] and isinstance(bucket[0][0], str)
                    and len(bucket[0][0]) > 3  # real words, not tokens
                    and not bucket[0][0].startswith("http")):
                    text_bucket_idx = idx
                    text = bucket[0][0]
                    if idx > 0 and isinstance(meta[2][idx - 1], list):
                        maybe_lang = self.safe_get_nested(meta, 2, idx - 1, 0)
                        if isinstance(maybe_lang, str) and len(maybe_lang) == 2:
                            language = maybe_lang
                    break

            # ------- user -----------------------------------------------------------
            user_block = self._find_user_meta(meta)
            if not user_block:
                return None

            user_name, profile_img, _, user_id = user_block[:4]
            total_reviews = user_block[5] if len(user_block) > 5 else 0
            is_local_guide = bool(user_block[12] if len(user_block) > 12 else 0)

            # ------- likes, images, replies ----------------------------------------
            likes = self._find_likes(meta)
            images = [s for s, p in self._long_strings(meta)
                      if s.startswith("https://lh3.googleusercontent.com/geougc-cs")
                      or s.startswith("https://lh3.googleusercontent.com/places/")]

            owner_response = None
            if text_bucket_idx is not None and text_bucket_idx + 1 < len(meta[2]):
                reply_bucket = meta[2][text_bucket_idx + 1]
                if (isinstance(reply_bucket, list) and reply_bucket
                    and isinstance(reply_bucket[0], list)
                    and reply_bucket[0] and isinstance(reply_bucket[0][0], str)):
                    resp = reply_bucket[0][0]
                    if any(w in resp.lower() for w in ("thank", "sorry", "appreciate", "glad")):
                        owner_response = resp

            # ------- timestamps -----------------------------------------------------
            micros = self.safe_get_nested(meta, 1, 2)  # 3rd item inside meta[1]
            published = self.datetime_from_microseconds(micros)

            # ------- build Review ---------------------------------------------------
            return Review(
                reviewId=str(meta[0]),
                reviewerId=str(user_id),
                reviewerName=str(user_name),
                stars=int(stars),
                text=text,
                language=language,
                publishedAtDate=published,
                likesCount=int(likes),
                ownerResponse=owner_response,
                images=images,
                reviewerPhotoUrl=profile_img,
                reviewerNumberOfReviews=int(total_reviews or 0),
                isLocalGuide=is_local_guide,
                sortDirection=direction,
                extractionConfidence=1.0,
                timeAgo="",  # add if you need it
                hasImages=bool(images),
                hasOwnerResponse=owner_response is not None,
            )
            
        except Exception as e:
            print(f"Error parsing review: {e}")
            return None

    def parse_batch(self, response_body: str, direction: str) -> tuple[List[Review], Optional[str]]:
        """Parse a batch of reviews from response body"""
        try:
            # Strip RPC prefix and parse JSON
            clean_json = self.strip_rpc_prefix(response_body)
            data = json_loads(clean_json)
            
            if not isinstance(data, list) or len(data) < 3:
                return [], None
            
            # Extract next token (stable position at index 1)
            next_token = data[1] if len(data) > 1 else None
            
            # Extract reviews list (stable position at index 2)
            reviews_list = data[2] if len(data) > 2 else []
            if not reviews_list:
                return [], next_token
            
            parsed_reviews = []
            duplicates_in_batch = 0
            
            for review_data in reviews_list:
                if not isinstance(review_data, list):
                    continue
                    
                review = self.fast_parse_review(review_data, direction)
                if not review:
                    continue
                
                # Check for duplicates
                if review.reviewerId in self.seen_reviewer_ids:
                    duplicates_in_batch += 1
                    self.duplicate_count += 1
                    continue
                
                # Add to results
                self.seen_reviewer_ids.add(review.reviewerId)
                parsed_reviews.append(review)
            
            # Update stats
            stats_key = 'highest_rating' if direction == 'HIGHEST' else 'lowest_rating'
            self.stats[stats_key]['reviews'] += len(parsed_reviews)
            self.stats[stats_key]['duplicates'] += duplicates_in_batch
            
            print(f"[{direction}] Parsed {len(parsed_reviews)} reviews, {duplicates_in_batch} duplicates")
            
            # Check if we should stop due to too many duplicates
            if duplicates_in_batch > self.duplicate_threshold:
                print(f"[{direction}] Stopping due to {duplicates_in_batch} duplicates in batch")
                self.stop_event.set()
            
            return parsed_reviews, next_token
            
        except Exception as e:
            print(f"Error parsing batch: {e}")
            return [], None

    async def producer(self, session: aiohttp.ClientSession, sort_by_highest: bool):
        """Producer coroutine - fetches data and puts it in queue"""
        direction = "HIGHEST" if sort_by_highest else "LOWEST"
        token = None
        page = 1
        
        print(f"[{direction}] Producer started")
        
        while not self.stop_event.is_set():
            try:
                # Build query and make request
                querystring = self.build_querystring(token, sort_by_highest)
                
                print(f"[{direction}] Fetching page {page}")
                async with session.get(self.base_url, params=querystring) as response:
                    if response.status != 200:
                        print(f"[{direction}] Request failed with status {response.status}")
                        break
                    
                    body = await response.text()
                    
                    # Put in queue for processing
                    await self.queue.put((body, direction))
                    
                    # Quick check for next token to avoid processing in producer
                    try:
                        clean_json = self.strip_rpc_prefix(body)
                        data = json_loads(clean_json)
                        token = data[1] if len(data) > 1 else None
                        
                        if not token or token in self.used_tokens:
                            print(f"[{direction}] No more tokens, producer stopping")
                            break
                        
                        self.used_tokens.add(token)
                        
                    except Exception:
                        print(f"[{direction}] Error extracting token, stopping")
                        break
                
                # Update page stats
                stats_key = 'highest_rating' if sort_by_highest else 'lowest_rating'
                self.stats[stats_key]['pages'] = page
                page += 1
                
                # Respectful delay
                await asyncio.sleep(1)
                
            except Exception as e:
                print(f"[{direction}] Producer error: {e}")
                break
        
        print(f"[{direction}] Producer finished")

    async def consumer(self):
        """Consumer coroutine - processes data from queue"""
        print("Consumer started")
        
        while not (self.stop_event.is_set() and self.queue.empty()):
            try:
                # Get data from queue with timeout
                try:
                    body, direction = await asyncio.wait_for(self.queue.get(), timeout=5.0)
                except asyncio.TimeoutError:
                    if self.stop_event.is_set():
                        break
                    continue
                
                # Process the batch
                reviews, next_token = self.parse_batch(body, direction)
                
                # Add to results
                if reviews:
                    self.all_reviews.extend(reviews)
                    print(f"Consumer processed {len(reviews)} reviews. Total: {len(self.all_reviews)}")
                
                # Mark task as done
                self.queue.task_done()
                
            except Exception as e:
                print(f"Consumer error: {e}")
                break
        
        print("Consumer finished")

    async def run(self):
        """Main execution method using producer-consumer pattern"""
        print(f"Starting optimized scraping for place ID: 0x{self.place_id}")
        print(f"Using {self.num_workers} workers with queue size {self.queue.maxsize}")
        
        # Configure connection limits
        connector = aiohttp.TCPConnector(
            limit_per_host=3,  # Limit concurrent connections to Google
            ttl_dns_cache=300,
            use_dns_cache=True,
        )
        
        timeout = aiohttp.ClientTimeout(total=30)
        
        async with aiohttp.ClientSession(
            headers=self.headers,
            timeout=timeout,
            connector=connector
        ) as session:
            
            # Create producer tasks
            producer_highest = asyncio.create_task(
                self.producer(session, sort_by_highest=True)
            )
            producer_lowest = asyncio.create_task(
                self.producer(session, sort_by_highest=False)
            )
            
            # Create consumer tasks
            consumers = [
                asyncio.create_task(self.consumer())
                for _ in range(self.num_workers)
            ]
            
            # Wait for producers to finish
            await asyncio.gather(producer_highest, producer_lowest, return_exceptions=True)
            
            # Wait for queue to be processed
            await self.queue.join()
            
            # Stop consumers
            self.stop_event.set()
            await asyncio.gather(*consumers, return_exceptions=True)
        
        # Save results
        self.save_results()
        
        print(f"\n=== OPTIMIZED SCRAPING COMPLETE ===")
        print(f"Total reviews scraped: {len(self.all_reviews)}")
        print(f"Total duplicates found: {self.duplicate_count}")
        print(f"Stats per direction:")
        for direction, stats in self.stats.items():
            print(f"  {direction}: {stats['pages']} pages, {stats['reviews']} reviews, {stats['duplicates']} duplicates")

    def save_results(self):
        """Save results to JSON file"""
        results = {
            'place_id': f'0x{self.place_id}',
            'extraction_timestamp': datetime.now().isoformat(),
            'total_reviews': len(self.all_reviews),
            'duplicate_count': self.duplicate_count,
            'stats': self.stats,
            'reviews': [asdict(review) for review in self.all_reviews]
        }
        
        try:
            with open(self.output_file, 'w', encoding='utf-8') as f:
                json.dump(results, f, indent=2, ensure_ascii=False)
            print(f"✅ Results saved to: {self.output_file}")
        except Exception as e:
            print(f"Error saving results: {e}")

def main():
    """Main entry point"""
    place_id = input("Enter the place ID (e.g., 89c3ca9c11f90c25:0x6cc8dba851799f09): ").strip()
    
    # Clean the place ID
    if place_id.startswith("1s0x"):
        place_id = place_id[4:]
    elif place_id.startswith("0x"):
        place_id = place_id[2:]
    
    # Create and run optimized scraper
    scraper = OptimizedGoogleMapsReviewScraper(
        place_id=place_id,
        max_queue_size=30,
        num_workers=2
    )
    
    asyncio.run(scraper.run())

if __name__ == "__main__":
    main()
