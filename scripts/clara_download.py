import requests
import argparse
import os
import sys
from urllib.parse import urlparse

def login(username, password):
    """Authenticate and retrieve API token"""
    url = "https://api.kpndataregistry.org/api/login"
    headers = {
        "accept": "application/json",
        "Content-Type": "application/json"
    }
    payload = {
        "user_name": username,
        "password": password
    }

    response = requests.post(url, headers=headers, json=payload)
    if response.status_code != 200:
        print(f"Login failed: {response.status_code} - {response.text}")
        sys.exit(1)

    data = response.json()
    return data.get("user").get("api_token")

def get_datasets(api_token):
    """Retrieve datasets from the API"""
    url = "https://api.kpndataregistry.org/api/upload-hermes"
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {api_token}"
    }

    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        print(f"Failed to retrieve datasets: {response.status_code} - {response.text}")
        sys.exit(1)

    return response.json()

def filter_datasets(datasets, uploaded_by=None, status=None, phenotype=None, dataset_name=None):
    """Filter datasets based on specified criteria"""
    filtered = []

    for dataset in datasets:
        match = True

        if uploaded_by and dataset.get("uploaded_by") != uploaded_by:
            match = False
        if status and dataset.get("status") != status:
            match = False
        if phenotype and dataset.get("phenotype") != phenotype:
            match = False
        if dataset_name and dataset_name not in dataset.get("dataset_name", ""):
            match = False

        if match:
            filtered.append(dataset)

    return filtered

def download_file(api_token, dataset_id, output_dir="."):
    """Download the file for a specific dataset ID"""
    url = f"https://api.kpndataregistry.org/api/hermes/download/{dataset_id}"
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {api_token}"
    }

    # Make request with allow_redirects=False to get the redirect URL
    response = requests.get(url, headers=headers, allow_redirects=False)

    if response.status_code != 307:
        print(f"Download request failed for ID {dataset_id}: {response.status_code} - {response.text}")
        return False

    # Get the redirect URL (pre-signed S3 URL)
    redirect_url = response.headers.get("Location")
    if not redirect_url:
        print(f"No redirect URL found for dataset ID {dataset_id}")
        return False

    # Extract filename from URL or use dataset_id if not found
    parsed_url = urlparse(redirect_url)
    filename = os.path.basename(parsed_url.path) or f"dataset_{dataset_id}"

    # Download the file from the pre-signed URL
    file_response = requests.get(redirect_url)
    if file_response.status_code != 200:
        print(f"Failed to download file from S3 for dataset ID {dataset_id}: {file_response.status_code}")
        return False

    # Save the file
    output_path = os.path.join(output_dir, filename)
    with open(output_path, "wb") as f:
        f.write(file_response.content)

    print(f"Downloaded dataset {dataset_id} to {output_path}")
    return True

def main():
    parser = argparse.ArgumentParser(description="Download datasets from KPN Data Registry")
    parser.add_argument("username", help="Username for login")
    parser.add_argument("password", help="Password for login")
    parser.add_argument("--output", "-o", default=".", help="Output directory for downloaded files")
    parser.add_argument("--uploader", help="Filter by uploaded_by value")
    parser.add_argument("--phenotype", help="Filter by phenotype value")
    parser.add_argument("--status", help="Filter by status value")
    parser.add_argument("--name", help="Filter by dataset_name (partial match)")

    args = parser.parse_args()

    # Ensure output directory exists
    os.makedirs(args.output, exist_ok=True)

    # Login and get API token
    print("Logging in...")
    api_token = login(args.username, args.password)
    if not api_token:
        print("Failed to obtain API token")
        sys.exit(1)

    # Get datasets
    print("Retrieving datasets...")
    datasets = get_datasets(api_token)

    # Filter datasets
    filtered_datasets = filter_datasets(
        datasets,
        uploaded_by=args.uploader,
        status=args.status,
        phenotype=args.phenotype,
        dataset_name=args.name
    )

    print(f"Found {len(filtered_datasets)} datasets matching criteria")

    # Download each matching dataset
    for dataset in filtered_datasets:
        dataset_id = dataset.get("id")
        if dataset_id:
            print(f"Downloading dataset {dataset_id} - {dataset.get('dataset_name', 'Unknown')}")
            download_file(api_token, dataset_id, args.output)
        else:
            print(f"Dataset missing ID: {dataset}")

    print("Done!")

if __name__ == "__main__":
    main()
