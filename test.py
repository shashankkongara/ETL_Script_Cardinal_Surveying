import hashlib
import pandas as pd
import re
from datetime import datetime
import os

# Load the new data
new_data_path = './input/Pipeline Class Registration_v2.csv'
new_data = pd.read_csv(new_data_path)

# Helper function to convert month name to month number
def month_to_number(month):
    if isinstance(month, int):
        return month
    month = month.strip().lower()
    months = {
        'january': 1, 'february': 2, 'march': 3,
        'april': 4, 'may': 5, 'june': 6,
        'july': 7, 'august': 8, 'september': 9,
        'october': 10, 'november': 11, 'december': 12
    }
    return months.get(month, None)

# Helper function to extract profile name from LinkedIn URL
def extract_profile_name(url):
    if pd.isna(url) or url.strip() == '':
        return None
    url = url.replace(' ', '')
    pattern = re.compile(r'linkedin\.com/(?:in/)?([A-Za-z0-9\-]+)', re.IGNORECASE)
    match = pattern.search(url)
    if match:
        profile_name = match.group(1).lower()
        if 'feed' not in profile_name and 'games' not in profile_name:
            return match.group(1)
    return None

# Helper function to transform job status into binary format
def transform_job_status(status):
    return 1 if 'Seeking' in status else 0

# Helper function to create a custom student ID using only Student ID and birth month number
def create_custom_student_id(row):
    combined_string = f"{row['Student ID']}{row['Birth Month']}"
    return hashlib.sha256(combined_string.encode('utf-8')).hexdigest()

# Rename columns appropriately
new_data = new_data.rename(columns={
    'Username': 'Email Address',
    'StudentID': 'Student ID',
    'Job Status': 'Job Status',
    'Birth Month': 'Birth Month',
    'Which class session will you attend? (Select all that apply)': 'Class Sessions',
    'Programming Experience level (Any language)': 'Programming Experience',
    'Python Programming Experience level': 'Python Experience',
    'LinkedIn Profile URL': 'LinkedIn Profile URL',
    'Timestamp': 'Timestamp'  # Make sure this column exists
})

# # Ensure Timestamp column is present and populated
# if 'Timestamp' not in new_data.columns:
#     new_data['Timestamp'] = datetime.now()
# new_data['Timestamp'] = pd.to_datetime(new_data['Timestamp'])

# Perform basic transformations
new_data['Email Address'] = new_data['Email Address'].str.lower()
new_data['Birth Month Number'] = new_data['Birth Month'].apply(month_to_number)
new_data['LinkedIn Profile Name'] = new_data['LinkedIn Profile URL'].apply(extract_profile_name)
new_data['Student UUID'] = new_data.apply(create_custom_student_id, axis=1)
new_data['Seeking Job Status'] = new_data['Job Status'].apply(transform_job_status)
new_data['Programming Experience'] = new_data['Programming Experience'].map({
    'Beginner': 1, 'Capable': 2, 'Intermediate': 3, 'Effective': 4,
    'Experienced': 5, 'Advance': 6, 'Distinguished': 7
}).astype('Int64')
new_data['Python Experience'] = new_data['Python Experience'].map({
    'Zero Experience': 0, 'Beginner': 1, 'Capable': 2, 'Intermediate': 3,
    'Effective': 4, 'Experienced': 5, 'Advance': 6
}).astype('Int64')

# Filter to only job seekers
new_data = new_data[new_data['Seeking Job Status'] == 1]

# Calculate attendance
class_sessions = new_data['Class Sessions'].str.get_dummies(';')
new_data = pd.concat([new_data, class_sessions], axis=1)
new_data['Total Sessions Attended'] = class_sessions.sum(axis=1)
total_sessions = class_sessions.shape[1]
new_data['Percentage Attendance'] = (new_data['Total Sessions Attended'] / total_sessions) * 100

# Check if existing data file exists
if os.path.exists('./output/normalized_data.xlsx'):
    # Load existing data
    existing_data_path = './output/normalized_data.xlsx'
    existing_student_pii = pd.read_excel(existing_data_path, sheet_name='Student PII')
    existing_skills = pd.read_excel(existing_data_path, sheet_name='Skills')
    existing_student_social = pd.read_excel(existing_data_path, sheet_name='Student Social')
    existing_top_candidates = pd.read_excel(existing_data_path, sheet_name='Top Candidates')

    # Combine existing and new data
    combined_student_pii = pd.concat([existing_student_pii, new_data[['Student UUID', 'Student ID', 'Birth Month Number', 'Timestamp']]])
    combined_skills = pd.concat([existing_skills, new_data[['Student UUID', 'Programming Experience', 'Python Experience', 'Timestamp']]])
    combined_student_social = pd.concat([existing_student_social, new_data[['Student UUID', 'LinkedIn Profile Name', 'Email Address', 'Timestamp']]])
    combined_top_candidates = pd.concat([existing_top_candidates, new_data[new_data['Python Experience'] > 3][['Student UUID', 'Python Experience', 'Percentage Attendance', 'Timestamp']]])
else:
    # If the file doesn't exist, initialize with new data
    combined_student_pii = new_data[['Student UUID', 'Student ID', 'Birth Month Number', 'Timestamp']]
    combined_skills = new_data[['Student UUID', 'Programming Experience', 'Python Experience', 'Timestamp']]
    combined_student_social = new_data[['Student UUID', 'LinkedIn Profile Name', 'Email Address', 'Timestamp']]
    combined_top_candidates = new_data[new_data['Python Experience'] > 3][['Student UUID', 'Python Experience', 'Percentage Attendance', 'Timestamp']]

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
