import pandas as pd
import mysql.connector
import numpy as np
from flaml import AutoML
from datetime import date, timedelta, datetime

## Connect to database mysql and fetch sales dataset 
mydb = mysql.connector.connect(
  host="**********",
  port="3306",
  user="varun",
  password="*********",
  database='**********'
)
mycursor=mydb.cursor()
inventory=pd.read_sql_query('''
select month(invoice_dt) as month,year(invoice_dt) as year,cust_id,sum(net_amt) as purchase from walkips4_laravel6.t_invoice_hdr where invoice_dt>'2021-01-01' and invoice_dt<'2022-08-01' and cust_id is not null
group by month,year,cust_id        
''',mydb)
df=inventory.replace('null',np.nan)
df=df.fillna(0)

## Preprocessing of sales data
df.sort_values(['year','month'])
df = df.pivot(index=['year','month'], columns='cust_id', values='purchase')
df.replace([np.inf, -np.inf], np.nan, inplace=True)
df=df.reset_index()
df=df.replace('null',np.nan)

# Create test dataframe
X=df[['month','year']]
data={'month':[8],'year':[2022]}
X_test=pd.DataFrame(data)
my_date = date.today()
year,week_num1,day_of_week= my_date.isocalendar()


# Predict Sales of every product one by one and concate the result into forecast_df dataframe
forecast_df = pd.DataFrame() 
product_number=0
for i in df.columns[3:]:
  null=df[i].isnull().sum()
  if null<2:
      y=df[i].fillna(0)
     
      automl = AutoML()
      automl_settings = {
      "time_budget": 60,  # in seconds
      "metric": 'r2',
      "task": 'regression',
      "log_file_name": "california.log",
      }  
      automl.fit(X_train=X, y_train=y,**automl_settings)
      predicted_sale = automl.predict(X_test[['month','year']])
      print(automl.model.estimator)
      forecast=pd.DataFrame()
      forecast['Purchase']=np.rint(predicted_sale)
      forecast['Month']='8'
      forecast['cust_id']=i
      forecast_df = pd.concat((forecast_df, forecast[['cust_id','Month','Purchase']]),axis=0)
      print(forecast)
      product_number+=1
      print(product_number)

# Convert result datafarme into csv file 
data=forecast_df.sort_values('Purchase',ascending='false')
data.to_csv('result.csv')
 