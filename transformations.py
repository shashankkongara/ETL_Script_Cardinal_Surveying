import hashlib
import pandas as pd
import re

class DataTransformer:
    def __init__(self, data):
        self.data = data

    @staticmethod
    def month_to_number(month):
        """Convert month name to month number."""
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

    @staticmethod
    def extract_profile_name(url):
        """Extract profile name from LinkedIn URL."""
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

    @staticmethod
    def transform_job_status(status):
        """Transform job status into binary format."""
        return 1 if 'Seeking' in status else 0

    @staticmethod
    def create_custom_student_id(row):
        """Create a custom student ID using Student ID and birth month."""
        combined_string = f"{row['Student ID']}{row['Birth Month']}"
        return hashlib.sha256(combined_string.encode('utf-8')).hexdigest()

    def rename_columns(self):
        """Rename columns to more appropriate names."""
        self.data = self.data.rename(columns={
            'Username': 'Email Address',
            'StudentID': 'Student ID',
            'Job Status': 'Job Status',
            'Birth Month': 'Birth Month',
            'Which class session will you attend? (Select all that apply)': 'Class Sessions',
            'Programming Experience level (Any language)': 'Programming Experience',
            'Python Programming Experience level': 'Python Experience',
            'LinkedIn Profile URL': 'LinkedIn Profile URL',
            'Timestamp': 'Timestamp'  # Ensure this column exists
        })

    def perform_basic_transformations(self):
        """Perform basic transformations like lowercasing emails and converting months."""
        self.data['Email Address'] = self.data['Email Address'].str.lower()
        self.data['Birth Month Number'] = self.data['Birth Month'].apply(self.month_to_number)
        self.data['LinkedIn Profile Name'] = self.data['LinkedIn Profile URL'].apply(self.extract_profile_name)
        self.data['Student UUID'] = self.data.apply(self.create_custom_student_id, axis=1)
        self.data['Seeking Job Status'] = self.data['Job Status'].apply(self.transform_job_status)

    def map_experience_levels(self):
        """Map experience levels to numerical values."""
        programming_experience_mapping = {
            'Beginner': 1,
            'Capable': 2,
            'Intermediate': 3,
            'Effective': 4,
            'Experienced': 5,
            'Advance': 6,
            'Distinguished': 7
        }

        python_experience_mapping = {
            'Zero Experience': 0,
            'Beginner': 1,
            'Capable': 2,
            'Intermediate': 3,
            'Effective': 4,
            'Experienced': 5,
            'Advance': 6
        }

        self.data['Programming Experience'] = self.data['Programming Experience'].map(programming_experience_mapping).astype('Int64')
        self.data['Python Experience'] = self.data['Python Experience'].map(python_experience_mapping).astype('Int64')

    def filter_job_seekers(self):
        """Filter the data to include only job seekers."""
        self.data = self.data[self.data['Seeking Job Status'] == 1]

    def calculate_attendance(self):
        """Calculate total sessions attended and percentage attendance."""
        class_sessions = self.data['Class Sessions'].str.get_dummies(';')
        self.data = pd.concat([self.data, class_sessions], axis=1)
        self.data['Total Sessions Attended'] = class_sessions.sum(axis=1)
        total_sessions = class_sessions.shape[1]
        self.data['Percentage Attendance'] = (self.data['Total Sessions Attended'] / total_sessions) * 100

    def get_transformed_data(self):
        """Return the transformed data."""
        return self.data
