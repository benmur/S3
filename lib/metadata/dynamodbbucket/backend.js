const util = require('util');

import { errors } from 'arsenal';
import { DynamoDB } from 'aws-sdk';

import { ListBucketResult } from '../in_memory/ListBucketResult';
import { markerFilter, prefixFilter } from '../in_memory/bucket_utilities';
import getMultipartUploadListing from '../in_memory/getMultipartUploadListing';

import DynamoDBMapper from './mapper';
import BucketInfo from '../BucketInfo';
import config from '../../Config';

class DynamoDBBucketClient {
    constructor() {
        this.q = new DynamoDBMapper(config.dynamodb);

        this.tablesCreated = false;

        const dbConfig = {
            endpoint: config.dynamodb.endpoint,
            region: config.dynamodb.region,
            apiVersion: '2012-08-10',
        };

        if (config.dynamodb.accessKey) {
            dbConfig.accessKeyId = config.dynamodb.accessKey;
        }

        if (config.dynamodb.secretKey) {
            dbConfig.secretAccessKey = config.dynamodb.secretKey;
        }

        this.db = new DynamoDB(dbConfig);
    }

    initialize() {
//        this.ensureDB();
    }

    ensureBucket(bucketName, cb) {
        const q = this.q.bucketGet(bucketName);
        this.db.getItem(q, (err, data) => {
            if (err) {
                if (err.code === 'ResourceNotFoundException') {
                    return cb(errors.NoSuchBucket);
                }
                return cb(err);
            }
            if (data.Item === undefined) {
                return cb(errors.NoSuchBucket);
            }
            return cb(null);
        });
    }

    ensureDB(log, cb) {
        if (this.tablesCreated) {
            return cb();
        }
        const q = this.q.createBucketTable();
        this.db.createTable(q, (err) => {
            // XXX handle concurrent creation
            if (err) {
                if (err.code !== 'ResourceInUseException') {
                    log.error('dynamodb bucket table creation error', { error: err });
                    return cb(err);
                }
            }

            const q = this.q.createObjectTable();
            this.db.createTable(q, (err) => {
                // XXX handle concurrent creation
                if (err) {
                    if (err.code !== 'ResourceInUseException') {
                        log.error('dynamodb object table creation error', { error: err });
                        return cb(err);
                    }
                }
            });

            this.tablesCreated = true;
            return cb();
        });
    }

    createBucket(bucketName, bucketMD, log, cb) {
        const q = this.q.bucketPut(bucketName, bucketMD.serialize());
// XXX BucketAlreadyExists
        this.ensureDB(log, (err) => {
            if (err) {
                return cb(err);
            }
            this.db.putItem(q, (err, data) => {
                return cb(err, data);
            });
        });
    }

    getBucketAttributes(bucketName, log, cb) {
        this.ensureDB(log, (err) => {
            if (err) {
                return cb(err);
            }
            this.db.getItem(this.q.bucketGet(bucketName), (err, data) => {
                if (err) {
                    if (err.code === 'ResourceNotFoundException') {
                        return cb(errors.NoSuchBucket);
                    }
                    return cb(err);
                }
                if (data.Item === undefined) {
                    return cb(errors.NoSuchBucket);
                }
                return cb(null, BucketInfo.deSerialize(data.Item.md.S));
            });
        });
    }

    getBucketAndObject(bucketName, objName, log, cb, params) {
        const ret = {};
        this.getBucketAttributes(bucketName, log, (err, bucket) => {
            if (err) {
                return cb(err, ret);
            }
            ret.bucket = bucket.serialize(); //JSON.stringify(bucket);
            this.getObject(bucketName, objName, log, (err, obj) => {
                ret.obj = JSON.stringify(obj);
                if (err && !err.NoSuchKey) {
                    return cb(err, ret);
                }
                // XXX double stringify
                return cb(null, ret);
            }, params);
        });
    }

    putBucketAttributes(bucketName, bucketMD, log, cb) {
        this.ensureDB(log, (err) => {
            if (err) {
                return cb(err);
            }
            this.db.putItem(
                this.q.bucketPut(bucketName, bucketMD.serialize()),
                (err, data) => {
                    return cb(err, data);
            });
        });
    }

    deleteBucket(bucketName, log, cb) {
        this.ensureBucket(bucketName, (err) => {
            if (err) {
                return cb(err);
            }
            this.db.deleteItem(this.q.bucketDelete(bucketName), (err, data) => {
                if (err) {
                    return cb(err);
                }
                return cb();
            });
        });
    }

    putObject(bucketName, objName, objVal, log, cb, params) {
        this.ensureDB(log, (err) => {
            if (err) {
                return cb(err);
            }
            this.ensureBucket(bucketName, (err) => {
                if (err) {
                    return cb(err);
                }
                this.db.putItem(
                    this.q.objectPut(bucketName, objName, objVal),
                    (err, data) => {
                        return cb(err, data);
                    });
            });
        });
    }

    getObject(bucketName, objName, log, cb, params) {
        this.ensureDB(log, (err) => {
            if (err) {
                return cb(err);
            }
            const q = this.q.objectGet(bucketName, objName);
            this.db.getItem(q, (err, data) => {
                if (err) {
                    if (err.code === 'ResourceNotFoundException') {
                        return cb(errors.NoSuchKey);
                    }
                    return cb(err);
                }
                if (data.Item === undefined) {
                    log.info('NoSuchKey', { bucketName, objName, q });
                    return cb(errors.NoSuchKey);
                }
                return cb(null, JSON.parse(data.Item.md.S));
            });
        });
    }

    deleteObject(bucketName, objName, log, cb, params) {
        this.ensureBucket(bucketName, (err) => {
            if (err) {
                return cb(err);
            }
            const q = this.q.objectDelete(bucketName, objName);
            this.db.deleteItem(q, (err, data) => {
                if (err) {
                    return cb(err);
                }
                return cb();
            });
        });
    }

    listObject(bucketName, params, log, cb) {
        this.ensureDB(log, (err) => {
            if (err) {
                return cb(err);
            }
            const q = this.q.bucketGet(bucketName);
            this.db.getItem(q, (err, data) => {
                if (err) {
                    if (err.code === 'ResourceNotFoundException') {
                        return cb(errors.NoSuchBucket);
                    }
                    return cb(err);
                }
                if (data.Item === undefined) {
                    return cb(errors.NoSuchBucket);
                }

                const { prefix, marker, delimiter, maxKeys } = params;
                const q = this.q.bucketList(bucketName, marker);
                this.db.query(q, (err, data) => {
                    if (err) {
                        return cb(err);
                    }

                    const response = new ListBucketResult();
                    const numKeys = maxKeys === null ? defaultMaxKeys : maxKeys;

                    let keys = [];
                    const mdMap = new Map();
                    data.Items.forEach(item => {
                        keys.push(item.objectName.S);
                        mdMap.set(item.objectName.S, JSON.parse(item.md.S));
                    });
                    keys.sort();

                    // If marker specified, edit the keys array so it
                    // only contains keys that occur alphabetically after the marker
                    if (marker) {
                        keys = markerFilter(marker, keys);
                        response.Marker = marker;
                    }
                    // If prefix specified, edit the keys array so it only
                    // contains keys that contain the prefix
                    if (prefix) {
                        keys = prefixFilter(prefix, keys);
                        response.Prefix = prefix;
                    }
                    // Iterate through keys array and filter keys containing
                    // delimiter into response.CommonPrefixes and filter remaining
                    // keys into response.Contents
                    for (let i = 0; i < keys.length; ++i) {
                        const currentKey = keys[i];
                        // Do not list object with delete markers
                        if (response.hasDeleteMarker(currentKey, mdMap)) {
                            continue;
                        }
                        // If hit numKeys, stop adding keys to response
                        if (response.MaxKeys >= numKeys) {
                            response.IsTruncated = true;
                            response.NextMarker = keys[i - 1];
                            break;
                        }
                        // If a delimiter is specified, find its index in the
                        // current key AFTER THE OCCURRENCE OF THE PREFIX
                        let delimiterIndexAfterPrefix = -1;
                        let prefixLength = 0;
                        if (prefix) {
                            prefixLength = prefix.length;
                        }
                        const currentKeyWithoutPrefix = currentKey
                            .slice(prefixLength);
                        let sliceEnd;
                        if (delimiter) {
                            delimiterIndexAfterPrefix = currentKeyWithoutPrefix
                                .indexOf(delimiter);
                            sliceEnd = delimiterIndexAfterPrefix + prefixLength;
                            response.Delimiter = delimiter;
                        }
                        // If delimiter occurs in current key, add key to
                        // response.CommonPrefixes.
                        // Otherwise add key to response.Contents
                        if (delimiterIndexAfterPrefix > -1) {
                            const keySubstring = currentKey.slice(0, sliceEnd + 1);
                            response.addCommonPrefix(keySubstring);
                        } else {
                            response.addContentsKey(currentKey, mdMap);
                        }
                    }
                    return cb(null, response);
                });
            });
        });
    }

    listMultipartUploads(bucketName, listingParams, log, cb) {
        this.getBucketAttributes(bucketName, log, (err, bucket) => {
            if (bucket === undefined) {
                // no on going multipart uploads, return empty listing
                return cb(null, {
                    IsTruncated: false,
                    NextMarker: undefined,
                    MaxKeys: 0,
                });
            }
            return getMultipartUploadListing(bucket, listingParams, cb);
        });
    }
}

export default DynamoDBBucketClient;
