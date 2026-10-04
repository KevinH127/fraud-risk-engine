from geopy.distance import geodesic
import redis_features


def check_velocity(card_id: str, current_time: float, max_tx_allowed: int = 3) -> bool:
    """
    Flags as fraud if card has been swiped more than 3 times in 2 minutes.
    """
    count = redis_features.update_and_get_velocity(card_id, current_time, window_seconds=120)
    return count > max_tx_allowed


def check_impossible_travel(card_id: str, current_lat: float, current_lon: float, current_time: float) -> bool:
    """
    Calculates travel speed between current and previous transaction using Haversine geodesic distance.
    If implied travel speed > 900 km/h (commercial jet speed), flags as impossible travel.
    """
    last_loc = redis_features.get_last_location(card_id)
    
    # Always update the location in Redis for future checks
    redis_features.update_last_location(card_id, current_lat, current_lon, current_time)
    
    if not last_loc:
        return False  # First time seeing this card, no previous point to compare
        
    time_diff_hours = (current_time - last_loc["timestamp"]) / 3600.0
    if time_diff_hours <= 0:
        return True  # Zero time delta with distance = impossible
        
    prev_coords = (last_loc["lat"], last_loc["lon"])
    curr_coords = (current_lat, current_lon)
    
    distance_km = geodesic(prev_coords, curr_coords).kilometers
    implied_speed_kmh = distance_km / time_diff_hours
    
    # If speed > 900 km/h and distance > 100km, flag physical card cloning
    return implied_speed_kmh > 900 and distance_km > 100


def check_high_risk_mcc(mcc: int, amount: float) -> bool:
    """
    Flags large transactions at high-risk Merchant Category Codes (Crypto, Gambling).
    """
    HIGH_RISK_MCCS = {6051, 7995}
    return (mcc in HIGH_RISK_MCCS) and (amount > 1000.0)