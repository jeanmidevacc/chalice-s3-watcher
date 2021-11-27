import boto3
from datetime import datetime
from chalice import Chalice, Rate, Cron
import pytz
import json

def get_latest_file(bucket, prefix):
    import boto3
    import pytz
    s3_client = boto3.client('s3')
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    latest = {"LastModified" : datetime(2021,1,1).replace(tzinfo=pytz.UTC)}
    if "Contents" in response:
        if len(response['Contents']) > 0:
            all = response['Contents']        
            latest = max(all, key=lambda x: x['LastModified'])
    return latest

configuration = {}

app = Chalice(app_name="s3_watcher")
@app.schedule(Cron(55, '*/4', '*', '*', '?', '*'))
def periodic_task(event):
    client = boto3.client("sns",
    aws_access_key_id=configuration["aws_key"]["aws_access_key_id"],
    aws_secret_access_key=configuration["aws_key"]["aws_secret_access_key"],
    region_name="us-east-1"
    )
    check_date = datetime.utcnow()
    for project, config in configuration["alerts"].items():
        bucket = config["bucket"]
        prefix = config["prefix"]
        time_threshold = config["time_threshold"]
        status = get_latest_file(bucket, prefix + f"{check_date.strftime('%Y%m%d')}")
        delta_time = check_date.replace(tzinfo=pytz.UTC) - status["LastModified"]
        
        message = f"ALERT !!!! The last file on the project {project} is late"
        if delta_time.seconds > time_threshold:
            client.publish(PhoneNumber=configuration["phone_number"], Message=message)

    return {"hello": "world"}


    
