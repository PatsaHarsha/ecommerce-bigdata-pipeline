import boto3

s3 = boto3.client('s3',
    endpoint_url='http://localhost:9000',
    aws_access_key_id='admin',
    aws_secret_access_key='bigdata123'
)

for bucket in s3.list_buckets()['Buckets']:
    print(f"Bucket: {bucket['Name']}")
    objects = s3.list_objects_v2(Bucket=bucket['Name'])
    if 'Contents' in objects:
        for obj in objects['Contents']:
            print(f"  - {obj['Key']}")
    else:
        print("  (Empty)")
