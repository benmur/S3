export default class DynamoDBMapper {
    constructor(config) {
        this.config = config;
    }

    createObjectTable() {
        return {
            TableName: this.config.objects.tableName,
            KeySchema: [
                { AttributeName: 'bucketName', KeyType: 'HASH' },
                { AttributeName: 'objectName', KeyType: 'RANGE' },
            ],
            AttributeDefinitions: [
                { AttributeName: 'bucketName', AttributeType: 'S' },
                { AttributeName: 'objectName', AttributeType: 'S' },
            ],
            ProvisionedThroughput: {
                ReadCapacityUnits: 1,
                WriteCapacityUnits: 1,
            },
        };
    }

    createBucketTable() {
        return {
            TableName: this.config.buckets.tableName,
            KeySchema: [
                { AttributeName: 'bucketName', KeyType: 'HASH' },
            ],
            AttributeDefinitions: [
                { AttributeName: 'bucketName', AttributeType: 'S' },
            ],
            ProvisionedThroughput: {
                ReadCapacityUnits: 1,
                WriteCapacityUnits: 1,
            },
        };
    }

    bucketGet(name) {
        return {
            Key: {
                bucketName: {
                    S: name,
                },
            },
            TableName: this.config.buckets.tableName,
            ConsistentRead: true,
            ProjectionExpression: "md",
        };
    }

    bucketPut(name, md) {
        // XXX add non-existence validation to handle separately
        // putBucketAttributes and createBucket
        return {
            Item: {
                bucketName: {
                    S: name,
                },
                md: {
                    S: md,
                },
            },
            TableName: this.config.buckets.tableName,
        };
    }

    objectPut(bucketName, objName, md) {
        return {
            Item: {
                bucketName: {
                    S: bucketName,
                },
                objectName: {
                    S: objName,
                },
                md: {
                    S: JSON.stringify(md),
                },
            },
            TableName: this.config.objects.tableName,
        };
    }

    bucketList(bucketName, marker = "") {
        const q = {
            TableName: this.config.objects.tableName,
            ConsistentRead: true,
            KeyConditionExpression: 'bucketName = :name',
            ExpressionAttributeValues: {
                ":name": {
                    S: bucketName,
                },
            },
        };

        if (marker) {
            q.ExclusiveStartKey = {
                bucketName: {
                    S: bucketName,
                },
                objectName: {
                    S: marker,
                },
            };
        }

        return q;
    }

    objectGet(bucketName, objectName) {
        return {
            Key: {
                bucketName: {
                    S: bucketName,
                },
                objectName: {
                    S: objectName,
                },
            },
            TableName: this.config.objects.tableName,
            ConsistentRead: true,
            ProjectionExpression: "md",
        };
    }

    objectDelete(bucketName, objectName) {
        return {
            Key: {
                bucketName: {
                    S: bucketName,
                },
                objectName: {
                    S: objectName,
                },
            },
            TableName: this.config.objects.tableName,
        };
    }

    bucketDelete(bucketName) {
        return {
            Key: {
                bucketName: {
                    S: bucketName,
                },
            },
            TableName: this.config.buckets.tableName,
        };
    }
}
