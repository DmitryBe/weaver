MATCH (c:City {id: $city_id})<-[:IN_CITY]-(r:Restaurant)<-[:HAS_RESTAURANT]-(b:Brand)
WITH b.id AS brand_id, b.name AS brand_name, count(r) AS restaurantCount
WITH collect({brand_id: brand_id, brand_name: brand_name, restaurantCount: restaurantCount}) AS brands,
     max(restaurantCount) AS maxCount
UNWIND brands AS row
RETURN
    row.brand_id    AS brand_id,
    row.brand_name  AS brand_name,
    round(toFloat(row.restaurantCount) / maxCount, 4) AS score
ORDER BY score DESC
LIMIT 5