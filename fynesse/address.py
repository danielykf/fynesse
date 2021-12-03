import statsmodels.api as sm
import numpy as np
import pandas as pd
import math
from sklearn.metrics import mean_squared_error

from .assess import DataPipeline


class PricePredictionModel:
  def __init__(self, data_pipeline: DataPipeline):
    self.data_pipeline = data_pipeline
    self.model = None
    self.rmse = None  # Root-mean-squared error
    self.nrmse = None   # Normalised root-mean-squared-error
  
  def predict_price(self, latitude, longitude, date, property_type):
    x, y = self.data_pipeline.get_dataset(latitude, longitude, date, property_type)
    if len(y) == 0:
      print("[Error: The prediction cannot be made.] No associated datapoints are found.")
      return
    
    # Split the dataset into train and test folds
    train_size = math.ceil(len(y) * 0.8)
    x_train, y_train = x[:train_size], y[:train_size]
    x_test, y_test = x[train_size:], y[train_size: ]
    
    # Train the model
    self.model = sm.OLS(y_train, x_train)
    self.model = self.model.fit_regularized(alpha=.01, L1_wt=0)
    
    # Validate the model
    if len(y_test) > 0:
      y_pred = self.model.predict(x_test)
      self.rmse = mean_squared_error(y_test, y_pred, squared=False)
      self.nrmse = self.rmse / (np.max(y_test) - np.min(y_test))
    
    if len(y) < 50 or (self.nrmse is not None and self.nrmse > 0.5):
      print(f"[Warning: The model might be inaccurate.] Number of datapoints = {len(y)}; Normalised-RMSE = {self.nrmse}")
    
    # Make prediction
    data_pipeline_predict = DataPipeline(self.data_pipeline.properties, self.data_pipeline.postcode, self.data_pipeline.prices_coordinates)
    data_pipeline_predict.set_bounding_box(latitude, longitude, 0.1, 0.1)
    df = data_pipeline_predict.join_df_with_pois(pd.DataFrame({'latitude': [latitude], 'longitude': [longitude]}))    
    return self.model.predict([[0, 0, df.loc[0, "poi_count"], 0, 1]])[0]

  def __getattr__(self, attr):
     return getattr(self.model, attr)