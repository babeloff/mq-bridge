-- TechEmpower `World` table: 10,000 rows of (id, randomNumber in 1..10000).
CREATE TABLE IF NOT EXISTS world (
    id integer NOT NULL PRIMARY KEY,
    randomnumber integer NOT NULL DEFAULT 0
);

INSERT INTO world (id, randomnumber)
SELECT g, floor(random() * 10000)::int + 1
FROM generate_series(1, 10000) AS g
ON CONFLICT (id) DO NOTHING;
