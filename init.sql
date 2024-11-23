CREATE TABLE rental_data (
    id UUID PRIMARY KEY,
    data VARCHAR(256) NOT NULL,
    most_specific_region_id INTEGER NOT NULL,
    start_date TIMESTAMP NOT NULL,
    end_date TIMESTAMP NOT NULL,
    number_seats INTEGER NOT NULL,
    price INTEGER NOT NULL,
    car_type TEXT NOT NULL,
    has_vollkasko BOOLEAN NOT NULL,
    free_kilometers INTEGER NOT NULL
);
