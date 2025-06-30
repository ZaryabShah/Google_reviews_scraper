from dataclasses import dataclass
from typing import List

@dataclass
class GoogleMapReviewData:
    reviewerId: str
    reviewerUrl: str
    reviewerName: str
    reviewId: str
    reviewUrl: str
    publishedAtDate: str
    placeId: str
    cid: str
    fid: str
    totalScore: float

    # Optional extras:
    text: str
    photos: List[str]
    likes_count: int
