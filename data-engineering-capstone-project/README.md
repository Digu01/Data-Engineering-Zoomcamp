Predicting Apartment Prices in Mexico City:

Preparation of Data for Apartment price in Mexico City.
Write a wrangle function that takes the name of a CSV file as input and returns a DataFrame. The function should do the following steps:

Subset the data in the CSV file and return only apartments in Mexico City ("Distrito Federal") that cost less than $100,000.
Remove outliers by trimming the bottom and top 10% of properties in terms of "surface_covered_in_m2".
Create separate "lat" and "lon" columns.
Mexico City is divided into 15 boroughs. Create a "borough" feature from the "place_with_parent_names" column.
Drop columns that are more than 50% null values.
Drop columns containing low- or high-cardinality categorical values.
Drop any columns that would constitute leakage for the target "price_aprox_usd".
Drop any columns that would create issues of multicollinearity.



Use glob to create the list files. It should contain the filenames of all the Mexico City real estate CSVs in the ./data directory, except for mexico-city-test-features.csv
