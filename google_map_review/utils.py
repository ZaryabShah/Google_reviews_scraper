def generate_urls_from_place_ids(place_ids):
    return [f"https://www.google.com/maps/place/?q=place_id:{pid}" for pid in place_ids]