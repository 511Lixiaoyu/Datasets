# -*- coding: utf-8 -*-
"""
Created on Mon Nov  4 12:49:47 2024

@author: Prince_Li
"""

import pandas as pd
import numpy as np
import h5py

save_flag = 0

def delete0(A):
    
    Am = A.shape[0]
    An = A.shape[1]
    for i in range(Am):
        for j in range(3,An):
            if A.iloc[i,j] == 0 and i!=0 and i!=Am-1:
                #  j!=3 and j!=An-1 and 
                # A[i,j] = (A[i-1,j]+A[i+1,j])/2
                A.iloc[i,j] = (A.iloc[i-1,j] + A.iloc[i+1,j] ) # /2
            elif A.iloc[i,j] == 0 and i==0:
                A.iloc[i,j] = (A.iloc[i+1,j]+A.iloc[i+2,j]) # /2
            elif A.iloc[i,j] == 0 and i==Am-1:
                A.iloc[i,j] = (A.iloc[i-1,j]+A.iloc[i-2,j]) # /2
            elif A.iloc[i,j] == 0 and j==3 and i<Am-2:
                A.iloc[i,j] = (A.iloc[i+1,j]+A.iloc[i+2,j]) # /2        
            elif A.iloc[i,j] == 0 and j==An-1 and i<Am-2:
                A.iloc[i,j] = (A.iloc[i+1, j]+A.iloc[i+1,j]) # /2           
    return A

# read Excel
df = pd.read_excel('new_test_20170508_20170512.xlsx')

# group by: mean value of 4-6 columns 
# columns name: 'day', 'interval', 'detid', 'occ', 'occ_100', 'flow', 'error', 'city', 'speed'
grouped_df = df.groupby(['day', 'interval', 'detid'])[['occ', 'occ_100', 'flow']].mean().reset_index()


# grouped_df_sorted = grouped_df.sort_values(by=['detid', 'day', 'interval'])

# missing data
all_days      = grouped_df['day'].unique()
all_intervals = grouped_df['interval'].unique()
all_detids    = grouped_df['detid'].unique()

all_combinations  = pd.MultiIndex.from_product([all_days, all_intervals,all_detids], names = ['day', 'interval', 'detid'])

# DataFrame
complete_df = pd.DataFrame(index=all_combinations).reset_index()

# left merge
merged_df = pd.merge(complete_df, grouped_df, on=['day', 'interval', 'detid'], how='left')

# NAN——>0
merged_df[['occ', 'occ_100', 'flow']] = merged_df[['occ', 'occ_100', 'flow']].fillna(0)

# sorted by 3,1,2 columns
merged_df_sorted = merged_df.sort_values(by=['detid', 'day', 'interval'])

# 删除缺失很多数据的detector
column_to_check  = 'detid'
values_to_delete = ['C1W-1','A5-6','A5-3','A5-2','A5-1', 'A4-2', 'A4-1', \
                    'A2-42', 'A2-12', 'A2-11', 'A10-32', 'A10-31', 'A1-5', \
                    'A1-3', 'A2-41', 'A4-3', 'A4-4', 'A4-5', 'A4-6', \
                    'A5-7', 'A5-8', 'B1-3', 'B14-5', 'B5-6', 'B7-2',\
                    'B7-3', 'B9-3','B9-3a', 'B14-2', 'B5-5', 'B5-7', 'B14-6', 'B7-4']

merged_df_sorted = merged_df_sorted[~merged_df_sorted[column_to_check].isin(values_to_delete)]

merged_df_sorted_data = delete0(merged_df_sorted) # [:,[range(3,6)]])



# density
# eq: k = 52.8*occ_100/(Lv+Ld)
Lv = 16
Ld = 6
merged_df_sorted['density']   = 52.8*merged_df_sorted['occ_100']/(Lv+Ld)
merged_df_sorted['flow_hour'] = merged_df_sorted['flow']*12

# speed
merged_df_sorted['speed']     =\
    np.minimum(merged_df_sorted['flow_hour']/merged_df_sorted['density'],60-np.random.random())

# results——>excel
merged_df_sorted.to_excel('new_new_test_20170508_20170512.xlsx', index=False)


# optimization model: flow_X, flow_Y
Z         = 147-len(values_to_delete)
T         = 960
M         = 24*16
utd19_flow_X   = np.zeros([Z,T,M])
utd19_flow_Y   = np.zeros([Z,T])
data_merged_df_sorted = merged_df_sorted.values
for i in range(Z):
    for j in range(T):
        utd19_flow_X[i,j,:] = data_merged_df_sorted[i*(5*288)+j:i*(5*288)+M+j, 5]
        utd19_flow_Y[i,j]   = data_merged_df_sorted[i*(5*288)+M+j+1, 5]

# optimization model: speed_X, speed_Y
utd19_speed_X   = np.zeros([Z,T,M])
utd19_speed_Y   = np.zeros([Z,T])
for i in range(Z):
    for j in range(T):
        utd19_speed_X[i,j,:] = data_merged_df_sorted[i*(5*288)+j:i*(5*288)+M+j, 8]
        utd19_speed_Y[i,j]   = data_merged_df_sorted[i*(5*288)+M+j+1, 8]

# UTD-flow
# save data
# input data
if save_flag == 1:
    f = h5py.File('utd19_flow_X.h5','w') 
    f['data'] = utd19_flow_X
    f.close()	
    # output data
    f = h5py.File('utd19_flow_Y.h5','w') 
    f['data'] = utd19_flow_Y
    f.close()	
    
    # UTD-speed
    f = h5py.File('utd19_speed_X.h5','w') 
    f['data'] = utd19_speed_X
    f.close()	
    # output data
    f = h5py.File('utd19_speed_Y.h5','w') 
    f['data'] = utd19_speed_Y
    f.close()	


utd19_flow_veiw01 = np.zeros([5*288,Z])
utd19_speed_veiw02 = np.zeros([5*288,Z])
for i in range(Z):
    utd19_flow_veiw01[:,i]  = data_merged_df_sorted[i*(5*288):(i+1)*(5*288),5]
    utd19_speed_veiw02[:,i] = data_merged_df_sorted[i*(5*288):(i+1)*(5*288),8]

import pandas as pd
utd19_flow_veiw01_excel = pd.DataFrame(utd19_flow_veiw01)
utd19_speed_veiw02_excel = pd.DataFrame(utd19_speed_veiw02)
utd19_flow_veiw01_excel.to_excel('utd19_flow_veiw01.xlsx', header=False, index=False)
utd19_speed_veiw02_excel.to_excel('utd19_speed_veiw02.xlsx', header=False, index=False)
