#!/usr/bin/env python
from __future__ import absolute_import
import boto3

rds_resource = boto3.client('rds')
db_instances = rds_resource.describe_db_instances()
db_clusters = rds_resource.describe_db_clusters()

# Set the tags - Instances and Aurora clusters must have these tags
tagKey = 'auto-stop'
tagValue = 'yes'

# Aurora may not be deployed in a particular region set to False in this case
aurora = True


def checkTag(resourceType,resourceArn,resourceIdentifier):
	db = resourceType + ' ' + resourceIdentifier
	dbResourceIdentifier = resourceIdentifier
	if resourceType == 'instance':
		db_tags = rds_resource.list_tags_for_resource(ResourceName=resourceArn)
		taglist = db_tags['TagList']

		if str(taglist) != '[]':
			for tag in taglist:
				if tag['Key'] == tagKey and tag['Value'] == tagValue:
					try:
						rds_resource.stop_db_instance(DBInstanceIdentifier=dbResourceIdentifier)
						break
					except Exception as e:
						print(e.message if hasattr(e, 'message') else e)
	else:
		if resourceType == 'cluster':
			cluster_tags = rds_resource.list_tags_for_resource(ResourceName=resourceArn)
			taglist = cluster_tags['TagList']
			if str(taglist) != '[]':
				for tag in taglist:
					if tag['Key'] == 'auto-stop' and tag['Value'] == 'yes':
						try:
							rds_resource.stop_db_cluster(DBClusterIdentifier=dbResourceIdentifier)
							break
						except rds_resource.exceptions.InvalidDBInstanceStateFault as error:
							if error.response['Error']['Code'] == 'InvalidDBInstanceState':
								print('Instance is in an invalid state. Unable to stop')
							else:
								raise error


for each_db in db_instances['DBInstances']:
	checkForTags = 'yes'
	message = ''
	db = str(each_db['DBInstanceIdentifier'])

	if (str(each_db['DBInstanceStatus']) == 'available') and (str(each_db['StorageType']) != 'aurora'):
		try:
			if (each_db['ReadReplicaDBInstanceIdentifiers'] is not None) or (each_db['ReadReplicaSourceDBInstanceIdentifier'] is not None):
					if  str(each_db['ReadReplicaDBInstanceIdentifiers']) != '[]':
						checkForTags = 'no'
					else:
						if str(each_db['ReadReplicaSourceDBInstanceIdentifier']) != '[]':
							checkForTags = 'no'
#						else:  #should be yes if RRI == '[]' and then RRS == '{}'
#							checkForTags = 'yes'  #redundant line
		except Exception as e:
			# Key is not present in JSON blob for some RDS engines
			checkForTags = 'yes'
			print(e.message if hasattr(e, 'message') else e)
	else:
		checkForTags = 'no'

	if checkForTags == 'yes':
		checkTag('instance',str(each_db['DBInstanceArn']),str(each_db['DBInstanceIdentifier']))

if aurora:
	for each_cluster in db_clusters['DBClusters']:
		checkForTags = 'yes'
		message = ''
		cluster = str(each_cluster['DBClusterIdentifier'])

		if (str(each_cluster['Status']) == 'available') and (str(each_cluster['EngineMode']) == 'provisioned'):
			try:
				if (each_cluster['ReadReplicaIdentifiers'] is not None) or (each_cluster['ReplicationSourceIdentifier'] is not None):
					if str(each_cluster['ReadReplicaIdentifiers']) != '[]':
						checkForTags = 'no'
			except Exception as e:
				print(e.message if hasattr(e, 'message') else e)
		else:
			checkForTags = 'no'

		if checkForTags == 'yes':
			checkTag('cluster', str(each_cluster['DBClusterArn']), cluster)

