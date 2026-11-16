# Congestion-zone pickup IDs, derived empirically (not from the official boundary).
# Rule: share of yellow pickups charged cbd_congestion_fee >= 0.90 with >= 100 trips,
# computed on 2025-03..2025-05. The next-highest zone is 0.61, so there is a clear gap.
# All 38 zones are Manhattan south of 60th St per taxi_zone_lookup.csv.
CBD_ZONES = [
    4, 12, 13, 45, 48, 50, 68, 79, 87, 88, 90, 100, 107, 113, 114, 125, 137, 144,
    148, 158, 161, 162, 163, 164, 170, 186, 209, 211, 224, 229, 230, 231, 232, 233,
    234, 246, 249, 261,
]