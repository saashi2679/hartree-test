# Implementation using Pandas Framework

import pandas as pd

print("Getting started with Pandas Framework")

dataframe_for_pandas_1 = pd.read_csv("dataset1.csv")
dataframe_for_pandas_2 = pd.read_csv("dataset2.csv")

print("Dropping unwanted column: invoice_id")
final_dataframe_for_pandas_1 = dataframe_for_pandas_1.drop('invoice_id',axis=1)

print("Transformation of Data:")
df_for_arap = final_dataframe_for_pandas_1[final_dataframe_for_pandas_1['status'] == 'ARAP']
df_for_accr = final_dataframe_for_pandas_1[final_dataframe_for_pandas_1['status'] == 'ACCR']

arap_df_grp = df_for_arap.groupby(['legal_entity','counter_party']).sum('value').rename(columns={'value':'arap_value'})
accr_df_grp = df_for_accr.groupby(['legal_entity','counter_party']).sum('value').rename(columns={'value':'accr_value'})

arap_df_grp = df_for_arap.groupby(['legal_entity','counter_party']).agg({'value':'sum', 'rating':'max'}).rename(columns={'value':'arap_value'})
accr_df_grp = df_for_accr.groupby(['legal_entity','counter_party']).agg({'value':'sum', 'rating':'max'}).rename(columns={'value':'accr_value'})
merged_df = pd.merge(arap_df_grp,accr_df_grp, on = ['legal_entity','counter_party'],how='outer')
merged_df_final = merged_df.rename(columns={'rating_x':'max_rating_x','arap_value_x':'arap_value','arap_value_y':'arap_value','rating_y':'max_rating_y',})
df_merged = merged_df_final.reset_index()

df_merged_final = df_merged.merge(dataframe_for_pandas_2,on=['counter_party'])

df_merged_final['ratings'] = df_merged_final[['max_rating_x','max_rating_y']].max(axis=1)
df_merged_final.fillna(0,inplace=True)

output_for_dataframe_final_1 = df_merged_final.drop(['max_rating_x','max_rating_y'],axis=1)
output_for_dataframe_final_1 = output_for_dataframe_final_1[['legal_entity','counter_party','tier','ratings','arap_value','accr_value']]
print("Final dataframe output:\n", output_for_dataframe_final_1)
output_for_dataframe_final_1.to_csv('result_pandas.csv')

print("\n\n DataFrame processing completed using Pandas Framework")


