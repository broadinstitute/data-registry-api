import json
import re

# Load coverage data
with open('coverage.json') as f:
    data = json.load(f)
    coverage_percent = data['totals']['percent_covered_display']

# Define the new badge URL
badge_url = f"https://img.shields.io/badge/coverage-{coverage_percent}%25-brightgreen"

# Read the README content
with open('README.md', 'r') as file:
    readme_content = file.read()

# Replace the existing badge in the README
new_readme_content = re.sub(
    r"https://img.shields.io/badge/coverage-[\d\\.]+%25-([a-z]+)",
    badge_url,
    readme_content
)

# Write back the updated README
with open('README.md', 'w') as file:
    file.write(new_readme_content)
