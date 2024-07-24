import pandas as pd
import os
from datetime import datetime
from transformations import DataTransformer

# Load the new data
new_data_path = './input/Pipeline Class Registration_v3.csv'
new_data = pd.read_csv(new_data_path)

# Initialize the DataTransformer class
transformer = DataTransformer(new_data)

# Perform transformations
transformer.rename_columns()
transformer.perform_basic_transformations()
transformer.map_experience_levels()
transformer.filter_job_seekers()
transformer.calculate_attendance()
transformed_data = transformer.get_transformed_data()

# Check if existing data file exists
if os.path.exists('./output/normalized_data.xlsx'):
    # Load existing data
    existing_data_path = './output/normalized_data.xlsx'
    existing_student_pii = pd.read_excel(existing_data_path, sheet_name='Student PII')
    existing_skills = pd.read_excel(existing_data_path, sheet_name='Skills')
    existing_student_social = pd.read_excel(existing_data_path, sheet_name='Student Social')
    existing_top_candidates = pd.read_excel(existing_data_path, sheet_name='Top Candidates')

    # Combine existing and new data
    combined_student_pii = pd.concat([existing_student_pii, transformed_data[['Student UUID', 'Student ID', 'Birth Month Number', 'Timestamp']]])
    combined_skills = pd.concat([existing_skills, transformed_data[['Student UUID', 'Programming Experience', 'Python Experience', 'Timestamp']]])
    combined_student_social = pd.concat([existing_student_social, transformed_data[['Student UUID', 'LinkedIn Profile Name', 'Email Address', 'Timestamp']]])
    combined_top_candidates = pd.concat([existing_top_candidates, transformed_data[transformed_data['Python Experience'] > 3][['Student UUID', 'Python Experience', 'Percentage Attendance', 'Timestamp']]])
else:
    # If the file doesn't exist, initialize with new data
    combined_student_pii = transformed_data[['Student UUID', 'Student ID', 'Birth Month Number', 'Timestamp']]
    combined_skills = transformed_data[['Student UUID', 'Programming Experience', 'Python Experience', 'Timestamp']]
    combined_student_social = transformed_data[['Student UUID', 'LinkedIn Profile Name', 'Email Address', 'Timestamp']]
    combined_top_candidates = transformed_data[transformed_data['Python Experience'] > 3][['Student UUID', 'Python Experience', 'Percentage Attendance', 'Timestamp']]

# Sort by Timestamp to ensure the latest records are on top
combined_student_pii = combined_student_pii.sort_values(by='Timestamp', ascending=False)
combined_skills = combined_skills.sort_values(by='Timestamp', ascending=False)
combined_student_social = combined_student_social.sort_values(by='Timestamp', ascending=False)
combined_top_candidates = combined_top_candidates.sort_values(by='Timestamp', ascending=False)

# Deduplicate based on the unique student identifier
deduplicated_student_pii = combined_student_pii.drop_duplicates(subset='Student UUID', keep='first')
deduplicated_skills = combined_skills.drop_duplicates(subset='Student UUID', keep='first')
deduplicated_student_social = combined_student_social.drop_duplicates(subset='Student UUID', keep='first')
deduplicated_top_candidates = combined_top_candidates.drop_duplicates(subset='Student UUID', keep='first')

# Save the normalized tables to an Excel file
output_file_path = './output/normalized_data.xlsx'
with pd.ExcelWriter(output_file_path, engine='openpyxl') as writer:
    deduplicated_student_pii.to_excel(writer, sheet_name='Student PII', index=False)
    deduplicated_skills.to_excel(writer, sheet_name='Skills', index=False)
    deduplicated_top_candidates.to_excel(writer, sheet_name='Top Candidates', index=False)
    deduplicated_student_social.to_excel(writer, sheet_name='Student Social', index=False)

print("Data has been normalized and saved to:", output_file_path)
