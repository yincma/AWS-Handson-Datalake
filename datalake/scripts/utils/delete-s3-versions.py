#!/usr/bin/env python3
import boto3
import sys

def delete_all_versions(bucket_name):
    s3 = boto3.client('s3')
    
    print(f"Deleting all versions from bucket: {bucket_name}")
    
    # List all object versions
    paginator = s3.get_paginator('list_object_versions')
    page_iterator = paginator.paginate(Bucket=bucket_name)
    
    delete_objects = []
    
    for page in page_iterator:
        # Process versions
        if 'Versions' in page:
            for version in page['Versions']:
                delete_objects.append({
                    'Key': version['Key'],
                    'VersionId': version['VersionId']
                })
        
        # Process delete markers
        if 'DeleteMarkers' in page:
            for marker in page['DeleteMarkers']:
                delete_objects.append({
                    'Key': marker['Key'],
                    'VersionId': marker['VersionId']
                })
        
        # Delete in batches of 1000 (AWS limit)
        if len(delete_objects) >= 1000:
            s3.delete_objects(
                Bucket=bucket_name,
                Delete={'Objects': delete_objects[:1000]}
            )
            print(f"Deleted {len(delete_objects[:1000])} objects")
            delete_objects = delete_objects[1000:]
    
    # Delete any remaining objects
    if delete_objects:
        s3.delete_objects(
            Bucket=bucket_name,
            Delete={'Objects': delete_objects}
        )
        print(f"Deleted {len(delete_objects)} objects")
    
    # Now delete the bucket
    try:
        s3.delete_bucket(Bucket=bucket_name)
        print(f"Successfully deleted bucket: {bucket_name}")
    except Exception as e:
        print(f"Error deleting bucket: {e}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python delete-s3-versions.py <bucket-name>")
        sys.exit(1)
    
    delete_all_versions(sys.argv[1])