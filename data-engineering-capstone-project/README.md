#Predicting Apartment Prices in Mexico City:

#Preparation of Data for Apartment price in Mexico City.
Write a wrangle function that takes the name of a CSV file as input and returns a DataFrame. The function should do the following steps:

Subset the data in the CSV file and return only apartments in Mexico City ("Distrito Federal") that cost less than $100,000.
Remove outliers by trimming the bottom and top 10% of properties in terms of "surface_covered_in_m2".
Create separate "lat" and "lon" columns.
Mexico City is divided into 15 boroughs. Create a "borough" feature from the "place_with_parent_names" column.
Drop columns that are more than 50% null values.
Drop columns containing low- or high-cardinality categorical values.
Drop any columns that would constitute leakage for the target "price_aprox_usd".
Drop any columns that would create issues of multicollinearity.



Use glob to create the list files. It should contain the filenames of all the Mexico City real estate CSVs in the ./data directory, except for mexico-city-test-features.csv.

Combine your wrangle function, a list comprehension, and pd.concat to create a DataFrame df. It should contain all the properties from the five CSVs in files.

#Create a histogram showing the distribution of apartment prices ("price_aprox_usd") in df. Be sure to label the x-axis "Price [$]", the y-axis "Count", and give it the title "Distribution of Apartment Prices". Use Matplotlib (plt).

![image](https://github.com/Digu01/Data-Engineering-Zoomcamp/assets/98606505/c6d55d80-ff6f-4615-ad54-f748442c9b3e)

Create a scatter plot that shows apartment price ("price_aprox_usd") as a function of apartment size ("surface_covered_in_m2"). Be sure to label your x-axis "Area [sq meters]" and y-axis "Price [USD]". Your plot should have the title "Mexico City: Price vs. Area". Use Matplotlib (plt).

Use your model to generate a Series of predictions for X_test. When you submit your predictions to the grader, it will calculate the mean absolute error for your model.

Create a scatter plot that shows apartment price ("price_aprox_usd") as a function of apartment size ("surface_covered_in_m2"). Be sure to label your x-axis "Area [sq meters]" and y-axis "Price [USD]". Your plot should have the title "Mexico City: Price vs. Area". Use Matplotlib (plt).

Create a horizontal bar chart that shows the 10 most influential coefficients for your model. Be sure to label your x- and y-axis "Importance [USD]" and "Feature", respectively, and give your chart the title "Feature Importances for Apartment Price". Use pandas.
