# s3trailparser
Parses Json Cloudtrail data into an elastic search cluster

<h2>Requires following lambda setup..</h2>
512-768 MB of memory preset
5 minutes runtime timeout

<h2>Following AWS services needed </h2>
Elastic Search with the following access policy below


{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "AWS": "*"
      },
      "Action": "es:*",
      "Resource": "<your ES cluster ARN>",
      "Condition": {
        "IpAddress": {
          "aws:SourceIp": "<whatever IP's you want to allow here careful with using * or 0.0.0.0/0>"
        }
      }
    },
    {
      "Effect": "Allow",
      "Principal": {
        "AWS": "*"
      },
      "Action": "es:*",
      "Resource": "<your ES cluster ARN>",
      "Condition": {
        "StringEquals": {
          "aws:UserAgent": "<secret string key to protect cluster API from being misused>"
        }
      }
    }
  ]
}

