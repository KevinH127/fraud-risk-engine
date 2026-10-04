import redis

# Connect to local Redis container
r = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)


def update_and_get_velocity(card_id: str, current_timestamp: float, window_seconds: int = 120) -> int:
    """
    Uses a Redis Sorted Set to track transactions within a rolling time window.
    Score = epoch timestamp, Value = unique tx timestamp.
    """
    key = f"card:{card_id}:tx_history"
    
    # Add current transaction timestamp
    r.zadd(key, {str(current_timestamp): current_timestamp})
    
    # Evict any transactions older than the window (e.g. older than 2 minutes)
    cutoff_time = current_timestamp - window_seconds
    r.zremrangebyscore(key, "-inf", cutoff_time)
    
    # Auto-expire the key if no activity for 10 minutes to save memory
    r.expire(key, 600)
    
    # Count how many transactions remain in this window
    return r.zcard(key)


def get_last_location(card_id: str):
    """
    Retrieves the last known latitude, longitude, and timestamp for this card.
    """
    key = f"card:{card_id}:last_location"
    data = r.hgetall(key)
    if not data:
        return None
    return {
        "lat": float(data["lat"]),
        "lon": float(data["lon"]),
        "timestamp": float(data["timestamp"]),
    }


def update_last_location(card_id: str, lat: float, lon: float, current_timestamp: float):
    """
    Updates the card's latest geographic position in Redis.
    """
    key = f"card:{card_id}:last_location"
    r.hset(key, mapping={"lat": lat, "lon": lon, "timestamp": current_timestamp})
    r.expire(key, 86400)  # Keep location history for 24 hours