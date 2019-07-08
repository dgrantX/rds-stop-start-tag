#!/usr/bin/env python

import boto3
#import logging

rds_resource = boto3.client('rds')
db_instances = rds_resource.describe_db_instances()

for each_db in db_instances['DBInstances']:
	db_tags = rds_resource.list_tags_for_resource(ResourceName=each_db['DBInstanceArn'])
	#print(db_tags)
	
	taglist = db_tags['TagList']
	for tag in taglist:
		if tag['Key'] == 'auto-stop' and tag['Value'] == 'yes' and each_db['DBInstanceStatus'] == 'available':
			db = each_db['DBInstanceIdentifier']
			status = each_db['DBInstanceStatus']
			print (db +':'+ status + ' and has Key ' + tag['Key'] + ' of value ' + tag['Value'])
			print (db + " will be shut down")
			try:
				rds_resource.stop_db_instance(DBInstanceIdentifier=db)
			except Exception as e:
				print(e.message if hasattr(e, 'message') else e)

#comment section out in regions where RDS clusters are not yet supported -v-
########
db_clusters = rds_resource.describe_db_clusters()

for each_cluster in db_clusters['DBClusters']:
	cluster_tags = rds_resource.list_tags_for_resource(ResourceName=each_cluster['DBClusterArn'])
	
	clusterTaglist = cluster_tags['TagList']
	for tag in clusterTaglist:
		if tag['Key'] == 'auto-stop' and tag['Value'] == 'yes' and each_cluster['Status'] == 'available':
			cluster = each_cluster['DBClusterIdentifier']
			status = each_cluster['Status']
			print (cluster +':'+ status + ' and has Key ' + tag['Key'] + ' of value ' + tag['Value'])
			print (cluster + " will be shut down")
			try:
				rds_resource.stop_db_cluster(DBClusterIdentifier=cluster)
			except Exception as e:
				print(e.message if hasattr(e, 'message') else e)
########
#comment section out in regions where RDS clusters are not yet supported -^-

return {
	'message' : 'complete'
}


