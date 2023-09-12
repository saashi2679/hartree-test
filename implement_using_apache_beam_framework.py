# Implementation using apache-beam framework:

import apache_beam as beam
import apache_beam.runners.interactive.interactive_beam as inter_beam
from apache_beam.runners.interactive.interactive_runner import InteractiveRunner as inter_runner

print("Getting started with Apache-beam Framework:")

print("Creating apache beam processing pipeline 1:")
pipeline1 = beam.Pipeline(inter_runner())

print("Creating apache beam processing pipeline 2:")
pipeline2 = beam.Pipeline(inter_runner())

dataframe_for_beam_1 = pipeline1 | 'Ingesting data from dataset1 csv file into first pipeline' >> beam.dataframe.io.read_csv('dataset1.csv')
dataframe_for_beam_2 = pipeline2 | 'Ingesting data from dataset2 csv file into second pipeline' >> beam.dataframe.io.read_csv('dataset2.csv')

data_frame_for_pipeline_1 = inter_beam.collect(dataframe_for_beam_1)
data_frame_for_pipeline_2 = inter_beam.collect(dataframe_for_beam_2)

print("Dropping unwanted column: invoice_id")
final_data_frame_for_pipeline_1 = data_frame_for_pipeline_1.drop('invoice_id',axis=1)

print("Transformation of Data:")
df_for_arap = final_data_frame_for_pipeline_1.query("status == 'ARAP'")
df_for_accr = final_data_frame_for_pipeline_1.query("status == 'ACCR'")
arap_df_grp = df_for_arap.pivot_table(values=['value', 'rating'], index=['legal_entity', 'counter_party'], aggfunc={'value':'sum', 'rating':'max'}).rename(columns={'value':'arap_value' , 'rating':'rating_x'})
accr_df_grp = df_for_accr.pivot_table(values=['value', 'rating'], index=['legal_entity', 'counter_party'], aggfunc={'value':'sum', 'rating':'max'}).rename(columns={'value':'accr_value', 'rating':'rating_y'})
merged_df = arap_df_grp.join(accr_df_grp, on=['legal_entity', 'counter_party'], how='outer').reset_index()

df_merged_final = merged_df.join(data_frame_for_pipeline_2.set_index('counter_party'), on='counter_party')
df_merged_final['ratings'] = df_merged_final[['rating_x','rating_y']].max(axis=1)
df_merged_final.fillna(0,inplace=True)
output_for_dataframe_final_1 = df_merged_final.drop(['rating_x','rating_y'],axis=1)
output_for_dataframe_final_1 = output_for_dataframe_final_1[['legal_entity','counter_party','tier','ratings','arap_value','accr_value']]


print(f"Dataframe Output obtained is:\n {output_for_dataframe_final_1}")
output_for_dataframe_final_1.to_csv('result_apache_beam.csv')

print("\n\n Pipeline processing completed using Apache-beam Framework")


