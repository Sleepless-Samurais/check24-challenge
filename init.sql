CREATE TABLE rental_data (
    id UUID PRIMARY KEY,
    data VARCHAR(512) NOT NULL,
    most_specific_region_id INTEGER NOT NULL,
    start_date TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    end_date TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    number_seats INTEGER NOT NULL,
    price INTEGER NOT NULL,
    car_type TEXT NOT NULL,
    has_vollkasko BOOLEAN NOT NULL,
    free_kilometers INTEGER NOT NULL
);

CREATE INDEX date_index on rental_data(start_date, end_date);
CREATE INDEX region_index on rental_data(most_specific_region_id);