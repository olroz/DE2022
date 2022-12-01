/*
 Завдання на SQL до лекції 02.
 */


/*
1.
Вивести кількість фільмів в кожній категорії.
Результат відсортувати за спаданням.
*/
-- SQL code goes here...

SELECT
    category.name category_name,
    count(film.film_id) films_count
FROM category
LEFT JOIN film_category ON
    category.category_id = film_category.category_id
LEFT JOIN film ON
    film_category.film_id = film.film_id
GROUP BY category.name
ORDER BY films_count DESC;






/*
2.
Вивести 10 акторів, чиї фільми брали на прокат найбільше.
Результат відсортувати за спаданням.
*/
-- SQL code goes here...

SELECT
    actor_name,
    taken
FROM (
    SELECT
        CONCAT(actor.first_name, ' ', actor.last_name) AS actor_name,
        COUNT(film_actor.actor_id) AS taken,
        RANK() OVER ( ORDER BY COUNT(film_actor.actor_id) DESC ) AS rnk
    FROM film_actor
    INNER JOIN inventory ON
        film_actor.film_id = inventory.film_id 
    INNER JOIN rental ON
        inventory.inventory_id  = rental.inventory_id
    INNER JOIN actor ON
        film_actor.actor_id = actor.actor_id
    GROUP BY
        film_actor.actor_id,
        actor.first_name,
        actor.last_name
    ORDER BY taken DESC
) tbl
WHERE rnk<=10






/*
3.
Вивести категорія фільмів, на яку було витрачено найбільше грошей
в прокаті
*/
-- SQL code goes here...

SELECT
    category.name,
    SUM(payment.amount) AS sum_rent
FROM rental
INNER JOIN inventory ON
    rental.inventory_id = inventory.inventory_id
INNER JOIN payment ON
    rental.rental_id  = payment.rental_id
INNER JOIN film_category ON
    inventory.film_id = film_category.film_id
INNER JOIN category ON
    category.category_id = film_category.category_id
GROUP BY category.name
ORDER BY sum_rent DESC
LIMIT 1






/*
4.
Вивести назви фільмів, яких не має в inventory.
Запит має бути без оператора IN
*/
-- SQL code goes here...

SELECT film.title 
FROM film
LEFT JOIN inventory ON film.film_id = inventory.film_id 
WHERE inventory.film_id IS NULL





/*
5.
Вивести топ 3 актори, які найбільше зʼявлялись в категорії фільмів “Children”.
*/
-- SQL code goes here...

SELECT actor_name, featured
FROM (
    SELECT 
        COUNT(film_actor.actor_id) AS featured,
        CONCAT(actor.first_name, ' ', actor.last_name) AS actor_name,
        RANK() OVER ( ORDER BY count(film_actor.actor_id) DESC ) AS rnk
    FROM film_actor
    INNER JOIN actor ON
        film_actor.actor_id = actor.actor_id
    WHERE film_actor.film_id IN 
    (
        SELECT film.film_id
        FROM film
        INNER JOIN film_category ON
            film.film_id = film_category.film_id
        INNER JOIN category ON
            film_category.category_id  = category.category_id 
        WHERE category.name = 'Children'
    )
    GROUP BY film_actor.actor_id, actor.first_name, actor.last_name
    ORDER BY featured DESC
) tbl
WHERE rnk<=3





/*
6.
Вивести міста з кількістю активних та неактивних клієнтів
(в активних customer.active = 1).
Результат відсортувати за кількістю неактивних клієнтів за спаданням.
*/
-- SQL code goes here...

SELECT 
	city.city, 
	SUM(CASE WHEN customer.active = 1 THEN 1 ELSE 0 END ) AS sum_active, 
	SUM(CASE WHEN customer.active = 0 THEN 1 else 0 END ) AS sum_inactive
from city
LEFT JOIN address ON
    city.city_id = address.city_id 
LEFT JOIN customer ON
    address.address_id  = customer.customer_id 
GROUP BY city.city
ORDER BY
    sum_inactive DESC,
    sum_active DESC


