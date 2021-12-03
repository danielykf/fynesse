# Repository overview

This repository provides the necessary toolings for building the housing price prediction model.

Link to github repo: [https://github.com/danielykf/fynesse](https://github.com/danielykf/fynesse)

## Access

There are 5 classes in `access.py`:
(1) `Database`: Stores the database connection and wraps around the database cursor.
(2) `Table`: Abstract class for the specific tables; Requires an implementation for `setup()`, which might include SQL operations such as table creation, data input and index creation.
(3) `PropertiesTable`, `PostcodeTable`, `PricesCoordinatesTable`: Subclasses of `Table`, representing each type of table present in the task.

Note that `Table` class also has methods `view` and `to_df`: The former is equivalent to `.head()` in panda dataframes, while the latter returns a pandas dataframe of the SQL table.

## Assess

There is only 1 class in `assess.py`, named as `DataPipeline`. In particular, `.get_dataset(latitude, longitude, date, property_type)` returns a labelled dataset, `x` and `y`. Briefly, for each query, the pipeline is as follows:
(1) Fix the bounding box using the coordinates of the point in the query.
(2) Join the properties and the postcode table on postcode to relate each property with its corresponding (latitude, longitude).
(3) Count the points-of-interest surrounding each property and make it as an additional feature.
(4) Re-base the data (i.e. latitude, longitude, date_of_transfer) using the query datapoint.

`DataPipeline` also comes with a `.plot()` function, showing the properties and points-of-interest on a map, with the properties coloured by their prices. It provides a good way to visualise the relations between property prices and the points-of-interest.

## Address

The price prediction model takes in the dataset generated from the `DataPipeline` class in `assess.py`, splits it into training/validation set in 80/20 split, and trains up and validates the model. If there are too few datapoints, or that the normalised root-mean-squared error of the predictions is > 0.5, the model signifies itself as "potentially inaccurate".

## Note

This model would not work well for areas with large variety of properties, for instance, different sizes, floors, etc. These factors could largely affect the pricing of a property, but no relevant data has been provided in this assignment.