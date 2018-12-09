
# coding: utf-8

# In[ ]:


# importing the packages
import pandas as pd
from datetime import date
from functools import reduce


# ### Q1

# In[ ]:


#importing all 4 data sets
jan=pd.read_csv("C://Users//rxr318//Desktop//OYO//TableA.csv")
feb=pd.read_csv("C://Users//rxr318//Desktop//OYO//TableB.csv")
march=pd.read_csv("C://Users//rxr318//Desktop//OYO//TableC.csv")
tableD=pd.read_excel("C://Users//rxr318//Desktop//OYO//TableD.xlsx")


# In[ ]:


#Checking the data types of all columns in jdataset jan
jan.dtypes


# In[ ]:


# convert checkin,checkout and date columns to date time from object for all tables
jan[["checkin", "checkout","date"]] = jan[["checkin", "checkout","date"]].apply(pd.to_datetime)
feb[["checkin", "checkout","date"]] = feb[["checkin", "checkout","date"]].apply(pd.to_datetime)
march[["checkin", "checkout","date"]] = march[["checkin", "checkout","date"]].apply(pd.to_datetime)


# In[ ]:


#checking datatypes again
jan.dtypes


# ### Q2

# In[ ]:


#unique users for each month
unique_users=pd.DataFrame()
unique_users_jan=pd.DataFrame({'jan_unique':jan['customer_id'].unique()})
unique_users_feb=pd.DataFrame({'feb_unique':feb['customer_id'].unique()})
unique_users_march=pd.DataFrame({'march_unique':march['customer_id'].unique()})

#concatinating unique users for each month into the same dataframe
unique_users=pd.concat([unique_users_jan,unique_users_feb,unique_users_march],axis=1)

#exporting list of unique users to csv
unique_users.to_csv("C://Users//rxr318//Desktop//OYO//Q2_Unique_users_monthly.csv")


# In[ ]:


#finding number of bookings and amount spent for each customer for jan, feb and march
jan_agg=jan.groupby(['customer_id'],as_index=False).agg({'booking_id':{'booking_id.jan':'nunique'},'amount':{'amount.jan':'sum'}})
feb_agg=feb.groupby(['customer_id'],as_index=False).agg({'booking_id':{'booking_id.feb':'nunique'},'amount':{'amount.feb':'sum'}})
march_agg=march.groupby(['customer_id'],as_index=False).agg({'booking_id':{'booking_id.march':'nunique'},'amount':{'amount.march':'sum'}})


# In[ ]:


#creating a function to calculate room nights for each booking
def _room_nights(dataset):
    #Converting checkout and checkin to datetime.date
    dataset['checkin']=dataset['checkin'].apply(lambda x:datetime.datetime.date(x))
    dataset['checkout']=dataset['checkout'].apply(lambda x:datetime.datetime.date(x))
    #Finding the room_nights and stoeing it in the respective dataset
    dataset['room_nights']=dataset['oyo_rooms']*(dataset['checkout']-dataset['checkin'])
    


# In[ ]:


#Finding roomnights for jan,feb,march
_room_nights(jan)
_room_nights(feb)
_room_nights(march)


# In[ ]:


#Checking the dataset for addition of a new column
jan.head()


# In[ ]:


#calculating total room nights stayed(status 2) for all users in all months and stoeing it in month_room_niht_stayed
jan_room_night_stayed=jan[jan['status']==2].groupby(['customer_id'],as_index=False).agg({'room_nights':{'room_night.jan':'sum'}})
feb_room_night_stayed=feb[feb['status']==2].groupby(['customer_id'],as_index=False).agg({'room_nights':{'room_night.feb':'sum'}})
march_room_night_stayed=feb[feb['status']==2].groupby(['customer_id'],as_index=False).agg({'room_nights':{'room_night.feb':'sum'}})


# ### Q3

# In[ ]:


#storing all individual datframes into a variable
dfs = [jan_agg,jan_room_night_stayed,feb_agg,feb_room_night_stayed,march_agg,march_room_night_stayed]


# In[ ]:


#merging all data sets together at customer level
df_final = reduce(lambda left,right: pd.merge(left,right,on='customer_id',how='outer'), dfs)


# In[ ]:


#dropping the first level of column names
df_final.columns=df_final.columns.droplevel(0)


# In[ ]:


#Renaming the first column of the final dataset
df_final.rename(columns={ df_final.columns[0]: "Customer_id" })
#Exporting the final datset to csv
df_final.to_csv("C://Users//rxr318//Desktop//OYO//Q3_Final_Dataset.csv")


# ### Q4

# In[ ]:


#repeat rate

#storing number of unique customers in january in variable X
X=jan['customer_id'].nunique()

#Storing number customers who shopped in Jan and then in feb in Y
Y=feb.loc[feb['customer_id'].isin(jan['customer_id'])]['customer_id'].nunique()


# In[ ]:


#finding repeat rate and storin it in X
repeat_rate=Y/X*100


# In[ ]:


#disply the repeat rate
repeat_rate


# ### Q5
# 

# In[ ]:


#taking the union of jan,feb and march data to find revenue for the entire time period
unionABC=pd.concat([jan,feb,march],ignore_index=True)


# In[ ]:


#joining the union table with city mapping
city_booking=pd.merge(left=unionABC,right=tableD,on='hotel_id',how='inner')


# In[ ]:


#finding the sum of amount, grouping it at city and hotel level, the aggregated column is renamed as amount_sum
city_agg=city_booking.groupby(['city','hotel_id']).agg({'amount':{'amount_sum':'sum'}})
#dropping the 1st level of column names after aggregation
city_agg.columns = city_agg.columns.droplevel(0)


# In[ ]:


#arranging the amount_sum column in descending order and keeping only top 3 rows for each city
top_3_citywise=city_agg.sort_values(['city','amount_sum'],ascending=False).groupby('city').head(3)


# In[ ]:


#exporting the final dataset for Q5 to csv
city_agg.to_csv("C://Users//rxr318//Desktop//OYO//Q5_top_3_citywise.csv")

