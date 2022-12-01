import pandas as pd
import boto3
from io import StringIO

data = [['AAA', 31], ['BBB', 30], ['CCC', 14], ['DDD', 30]]
df = pd.DataFrame(data, columns=['Name', 'Age'])
df

ACCESS_KEY = "AKIAWCM63MXFHX3IVAJK"
SECRET_KEY = "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"


def upload_s3(df):
    i = "test.csv"
    s3 = boto3.client("s3", aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)
    csv_buf = StringIO()
    df.to_csv(csv_buf, header=True, index=False)
    csv_buf.seek(0)
    s3.put_object(Bucket="storageclass-demo-aakash", Body=csv_buf.getvalue(), Key='uploaded/' + i)


######################SNS###############

ses = boto3.client('sns', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY, region_name='ap-south-1')
sns_topicname_arn = "arn:aws:sns:ap-south-1:417481844170:s3_upload_notif"


# Publish the message to the SNS topic

def publishMessage(snsArn, msg):
    client = boto3.client('sns', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY,
                          region_name='ap-south-1')
    client.publish(TargetArn=snsArn, Message=msg)


upload_s3(df)

msg = "file has been uploaded into your bucket"

publishMessage(sns_topicname_arn, msg)