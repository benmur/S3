'use strict';

const chai = require('chai');
const expect = chai.expect;
const crypto = require('crypto');
const parseString = require('xml2js').parseString;
const utils = require('../lib/utils.js');
const bucketPut = require('../lib/api/bucketPut.js');
const bucketHead = require('../lib/api/bucketHead.js');
const objectPut = require('../lib/api/objectPut.js');
const objectHead = require('../lib/api/objectHead.js');
const objectGet = require('../lib/api/objectGet.js');
const bucketGet = require('../lib/api/bucketGet.js');
const serviceGet = require('../lib/api/serviceGet.js');
const accessKey = 'accessKey1';
const namespace = 'default';

describe("bucketPut API",function(){
	let metastore;

	beforeEach(function () {
	   metastore = {
			  "users": {
			      "accessKey1": {
			        "buckets": []
			      },
			      "accessKey2": {
			        "buckets": []
			      }
			  },
			  "buckets": {}
			}
	});


	it("should return an error if no bucketname provided", function(done){

		const testRequest = {
			lowerCaseHeaders: {},
			 url: '/',
			 namespace: namespace,
			 post: ''
		}

		bucketPut(accessKey, metastore, testRequest, function(err, result) {
			expect(err).to.equal('Missing bucket name');
			done();
		})

	});

	it("should return an error if bucketname is invalid", function(done){

		const tooShortBucketName = 'hi';
		const testRequest = {
			lowerCaseHeaders: {},
			 url: `/${tooShortBucketName}`,
			 namespace: namespace,
			 post: ''
		}

		bucketPut(accessKey, metastore, testRequest, function(err, result) {
			expect(err).to.equal('Bucket name is invalid');
			done();
		})

	});

	it("should return an error if improper xml is provided in request.post", function(done){

		const testRequest = {
			lowerCaseHeaders: {},
			 url: '/test1',
			 namespace: namespace,
			 post: 'improperxml'
		}

		bucketPut(accessKey, metastore, testRequest, function(err, result) {
			expect(err).to.equal('Improper XML');
			done();
		})

	});


	it("should return an error if xml which does not conform to s3 docs is provided in request.post", function(done){

		const testRequest = {
			lowerCaseHeaders: {},
			 url: '/test1',
			 namespace: namespace,
			 post: '<Hello></Hello>'
		}

		bucketPut(accessKey, metastore, testRequest, function(err, result) {
			expect(err).to.equal('LocationConstraint improperly specified');
			done();
		})

	});


	it("should create a bucket using bucket name provided in path", function(done){

		const bucketName = 'test1'
		const testRequest = {
			lowerCaseHeaders: {},
			 url: `/${bucketName}`,
			 namespace: namespace,
			 post: ''
		}

		const testBucketUID = utils.getResourceUID(testRequest.namespace, bucketName);

		bucketPut(accessKey, metastore, testRequest, function(err, success) {
			expect(success).to.equal('Bucket created');
			expect(metastore.buckets[testBucketUID].name).to.equal(bucketName);
			expect(metastore.buckets[testBucketUID].owner).to.equal(accessKey);
			expect(metastore.users[accessKey].buckets).to.have.length.of.at.least(1);
			done();
		})

	});


	it("should create a bucket using bucket name provided in host", function(done){

		const bucketName = 'BucketName'
		const testRequest = {
			lowerCaseHeaders: {},
			 url: '/',
			 namespace: namespace,
			 post: '',
			 headers: {host: `${bucketName}.s3.amazonaws.com`}
		}

		const testBucketUID = utils.getResourceUID(testRequest.namespace, bucketName);

		bucketPut(accessKey, metastore, testRequest, function(err, success) {
			expect(success).to.equal('Bucket created');
			expect(metastore.buckets[testBucketUID].name).to.equal(bucketName);
			expect(metastore.buckets[testBucketUID].owner).to.equal(accessKey);
			expect(metastore.users[accessKey].buckets).to.have.length.of.at.least(1);
			done();
		})

	});
});

describe("bucketHead API",function(){

	let metastore;

	beforeEach(function () {
	   metastore = {
			  "users": {
			      "accessKey1": {
			        "buckets": []
			      },
			      "accessKey2": {
			        "buckets": []
			      }
			  },
			  "buckets": {}
			}
	});


	it("should return an error if the bucket does not exist", function(done){
		const bucketName = 'BucketName';
		const testRequest = {
			headers: {host: `${bucketName}.s3.amazonaws.com`},
			url: '/',
			namespace: namespace
		}

		bucketHead(accessKey, metastore, testRequest, function(err, result) {
			expect(err).to.equal('Bucket does not exist -- 404');
			done();
		})

	});

	it("should return an error if user is not authorized", function(done){
		const bucketName = 'BucketName';
		const putAccessKey = 'accessKey2';
		const testRequest = {
			lowerCaseHeaders: {},
			headers: {host: `${bucketName}.s3.amazonaws.com`},
			url: '/',
			namespace: namespace
		}

		bucketPut(putAccessKey, metastore, testRequest, function(err, success) {
			expect(success).to.equal('Bucket created');
			bucketHead(accessKey, metastore, testRequest, function(err, result) {
				expect(err).to.equal('Action not permitted -- 403');
				done();
			})
		})
	});

	it("should return a success message if bucket exists and user is authorized", function(done){
		const bucketName = 'BucketName';
		const testRequest = {
			lowerCaseHeaders: {},
			headers: {host: `${bucketName}.s3.amazonaws.com`},
			url: '/',
			namespace: namespace
		}

		bucketPut(accessKey, metastore, testRequest, function(err, success) {
			expect(success).to.equal('Bucket created');
			bucketHead(accessKey, metastore, testRequest, function(err, result) {
				expect(result).to.equal('Bucket exists and user authorized -- 200');
				done();
			})
		})
	});
});


describe("objectPut API",function(){

	let metastore;
	let datastore;

	beforeEach(function () {
	   metastore = {
			  "users": {
			      "accessKey1": {
			        "buckets": []
			      },
			      "accessKey2": {
			        "buckets": []
			      }
			  },
			  "buckets": {}
			};
		datastore = {};
	});


	it("should return an error if the bucket does not exist", function(done){
		const bucketName = 'BucketName';
		const postBody = 'I am a body';
		const testRequest = {
			headers: {host: `${bucketName}.s3.amazonaws.com`},
			url: '/',
			namespace: namespace,
			post: postBody
		}

		objectPut(accessKey, datastore, metastore, testRequest, function(err, result) {
			expect(err).to.equal('Bucket does not exist -- 404');
			done();
		})
	});


	it("should return an error if user is not authorized", function(done){
		const bucketName = 'BucketName';
		const postBody = 'I am a body';
		const putAccessKey = 'accessKey2';
		const testPutBucketRequest = {
			lowerCaseHeaders: {},
			headers: {host: `${bucketName}.s3.amazonaws.com`},
			url: '/',
			namespace: namespace,
		}
		const testPutObjectRequest = {
			lowerCaseHeaders: {},
			headers: {host: `${bucketName}.s3.amazonaws.com`},
			url: '/',
			namespace: namespace,
			post: postBody
		}

		bucketPut(putAccessKey, metastore, testPutBucketRequest, function(err, success) {
			expect(success).to.equal('Bucket created');
			objectPut(accessKey, datastore, metastore, testPutObjectRequest, function(err, result) {
				expect(err).to.equal('Action not permitted -- 403');
				done();
			})
		})
	});


	it("should return an error if Content MD-5 is invalid", function(done){
		const bucketName = 'BucketName';
		const postBody = 'I am a body';
		const incorrectMD5 = 'asdfwelkjflkjslfjskj993ksjl'
		const objectName = 'objectName';
		const testPutBucketRequest = {
			lowerCaseHeaders: {},
			headers: {host: `${bucketName}.s3.amazonaws.com`},
			url: '/',
			namespace: namespace,
		}
		const testPutObjectRequest = {
			lowerCaseHeaders: {
				'content-md5': incorrectMD5
			},
			headers: {host: `${bucketName}.s3.amazonaws.com`},
			url: `/${objectName}`,
			namespace: namespace,
			post: postBody
		}

		bucketPut(accessKey, metastore, testPutBucketRequest, function(err, success) {
			expect(success).to.equal('Bucket created');
			objectPut(accessKey, datastore, metastore, testPutObjectRequest, function(err, result) {
				expect(err).to.equal('Content-MD5 is invalid');
				done();
			})
		})
	});

	it.skip("should return an error if datastore reports an error back", function(){
		//TODO: Test to be written once services.putDataStore includes an actual call to 
		//datastore rather than just the in memory adding of a key/value pair to the datastore
		//object
	});

	it.skip("should return an error if metastore reports an error back", function(){
		//TODO: Test to be written once services.metadataStoreObject includes an actual call to 
		//datastore rather than just the in memory adding of a key/value pair to the datastore
		//object
	});


	it("should successfully put an object", function(done){
		const bucketName = 'BucketName';
		const postBody = 'I am a body';
		const correctMD5 = 'be747eb4b75517bf6b3cf7c5fbb62f3a';
		const bucketUID = '84d4cad3cdb50ad21b6c1660a92627b3'
		const objectUID = '84c130398c854348bcff8b715f793dc4'
		const objectName = 'objectName';
		const testPutBucketRequest = {
			lowerCaseHeaders: {},
			headers: {
				host: `${bucketName}.s3.amazonaws.com`
			},
			url: '/',
			namespace: namespace
		}
		const testPutObjectRequest = {
			lowerCaseHeaders: {},
			url: `/${bucketName}/${objectName}`,
			namespace: namespace,
			post: postBody,
			calculatedMD5: 'be747eb4b75517bf6b3cf7c5fbb62f3a'
		}

		bucketPut(accessKey, metastore, testPutBucketRequest, function(err, success) {
			expect(success).to.equal('Bucket created');
			objectPut(accessKey, datastore, metastore, testPutObjectRequest, function(err, result) {
				expect(result).to.equal(correctMD5);
				expect(metastore.buckets[bucketUID]['keyMap'][objectName]).to.exist;
				expect(metastore.buckets[bucketUID]['keyMap'][objectName]['content-md5']).to.equal(correctMD5);
				expect(datastore[objectUID]).to.equal('I am a body');
				done();
			})
		})
	});
	

	it("should successfully put an object with user metadata", function(done){
		const bucketName = 'BucketName';
		const postBody = 'I am a body';
		const correctMD5 = 'be747eb4b75517bf6b3cf7c5fbb62f3a';
		const bucketUID = '84d4cad3cdb50ad21b6c1660a92627b3'
		const objectName = 'objectName';
		const testPutBucketRequest = {
			lowerCaseHeaders: {},
			headers: {host: `${bucketName}.s3.amazonaws.com`},
			url: '/',
			namespace: namespace,
		}
		const testPutObjectRequest = {
			lowerCaseHeaders: {
				//Note that Node will collapse common headers into one 
				//(e.g. "x-amz-meta-test: hi" and "x-amz-meta-test: there" becomes "x-amz-meta-test: hi, there")
				//Here we are not going through an actual http request so will not collapse properly.  
				'x-amz-meta-test': 'some metadata',
				'x-amz-meta-test2': 'some more metadata',
				'x-amz-meta-test3': 'even more metadata',
			},
			url: `/${bucketName}/${objectName}`,
			namespace: namespace,
			post: postBody,
			calculatedMD5: 'be747eb4b75517bf6b3cf7c5fbb62f3a'
		}

		bucketPut(accessKey, metastore, testPutBucketRequest, function(err, success) {
			expect(success).to.equal('Bucket created');
			objectPut(accessKey, datastore, metastore, testPutObjectRequest, function(err, result) {
				expect(result).to.equal(correctMD5);
				expect(metastore.buckets[bucketUID]['keyMap'][objectName]).to.exist;
				expect(metastore.buckets[bucketUID]['keyMap'][objectName]['x-amz-meta-test']).to.equal('some metadata');
				expect(metastore.buckets[bucketUID]['keyMap'][objectName]['x-amz-meta-test2']).to.equal('some more metadata');
				expect(metastore.buckets[bucketUID]['keyMap'][objectName]['x-amz-meta-test3']).to.equal('even more metadata');
				done();
			})
		})
	});

});


describe("objectHead API",function(){

	let metastore;
	let datastore;

	beforeEach(function () {
	   metastore = {
			  "users": {
			      "accessKey1": {
			        "buckets": []
			      },
			      "accessKey2": {
			        "buckets": []
			      }
			  },
			  "buckets": {}
			};
		datastore = {};
	});

	const bucketName = 'BucketName';
	const postBody = 'I am a body';
	const correctMD5 = 'be747eb4b75517bf6b3cf7c5fbb62f3a';
	const incorrectMD5 = 'fkjwelfjlslfksdfsdfsdfsdfsdfsdj';
	const objectName = 'objectName';
	let date = new Date();
	let laterDate = date.setMinutes(date.getMinutes() + 30);
	let earlierDate = date.setMinutes(date.getMinutes() - 30);
	const testPutBucketRequest = {
		lowerCaseHeaders: {},
		url: `/${bucketName}`,
		namespace: namespace,
	};
	const userMetadataKey = 'x-amz-meta-test';
	const userMetadataValue = 'some metadata';
	const testPutObjectRequest = {
		lowerCaseHeaders: {
			'x-amz-meta-test': userMetadataValue
		},
		url: `/${bucketName}/${objectName}`,
		namespace: namespace,
		post: postBody,
		calculatedMD5: 'be747eb4b75517bf6b3cf7c5fbb62f3a'
	};


	it("should return 304 (not modified) if request header includes 'if-modified-since' \
		and object not modified since specified time", function(done){

		const testGetRequest = {
			lowerCaseHeaders: {
				'if-modified-since': laterDate
			},
			url: `/${bucketName}/${objectName}`,
			namespace: namespace
		};

		bucketPut(accessKey, metastore, testPutBucketRequest, function(err, success) {
			expect(success).to.equal('Bucket created');
			objectPut(accessKey, datastore, metastore, testPutObjectRequest, function(err, result) {
				expect(result).to.equal(correctMD5);
				objectHead(accessKey, metastore, testGetRequest, function(err, success) {
					expect(err).to.equal('Not modified -- 304');
					done();
				})
			})
		})
	});
	

	it("should return 412 (precondition failed) if request header includes 'if-unmodified-since' and \
		object has been modified since specified time", function(done){
		const testGetRequest = {
			lowerCaseHeaders: {
				'if-unmodified-since': earlierDate
			},
			url: `/${bucketName}/${objectName}`,
			namespace: namespace
		};

		bucketPut(accessKey, metastore, testPutBucketRequest, function(err, success) {
			expect(success).to.equal('Bucket created');
			objectPut(accessKey, datastore, metastore, testPutObjectRequest, function(err, result) {
				expect(result).to.equal(correctMD5);
				objectHead(accessKey, metastore, testGetRequest, function(err, success) {
					expect(err).to.equal('Precondition failed -- 412');
					done();
				})
			})
		})
	});


	it("should return 412 (precondition failed) if request header includes 'if-match' and \
		Etag of object does not match specified Etag", function(done){
		const testGetRequest = {
			lowerCaseHeaders: {
				'if-match': incorrectMD5
			},
			url: `/${bucketName}/${objectName}`,
			namespace: namespace
		};

		bucketPut(accessKey, metastore, testPutBucketRequest, function(err, success) {
			expect(success).to.equal('Bucket created');
			objectPut(accessKey, datastore, metastore, testPutObjectRequest, function(err, result) {
				expect(result).to.equal(correctMD5);
				objectHead(accessKey, metastore, testGetRequest, function(err, success) {
					expect(err).to.equal('Precondition failed -- 412');
					done();
				})
			})
		})				
	});


	it("should return 304 (not modified) if request header includes 'if-none-match' and \
		Etag of object does match specified Etag", function(done){
		const testGetRequest = {
			lowerCaseHeaders: {
				'if-none-match': correctMD5
			},
			url: `/${bucketName}/${objectName}`,
			namespace: namespace
		};

		bucketPut(accessKey, metastore, testPutBucketRequest, function(err, success) {
			expect(success).to.equal('Bucket created');
			objectPut(accessKey, datastore, metastore, testPutObjectRequest, function(err, result) {
				expect(result).to.equal(correctMD5);
				objectHead(accessKey, metastore, testGetRequest, function(err, success) {
					expect(err).to.equal('Not modified -- 304');
					done();
				})
			})
		})		
	});

	it("should get the object metadata", function(done){
		const testGetRequest = {
			lowerCaseHeaders: {},
			url: `/${bucketName}/${objectName}`,
			namespace: namespace
		};

		bucketPut(accessKey, metastore, testPutBucketRequest, function(err, success) {
			expect(success).to.equal('Bucket created');
			objectPut(accessKey, datastore, metastore, testPutObjectRequest, function(err, result) {
				expect(result).to.equal(correctMD5);
				objectHead(accessKey, metastore, testGetRequest, function(err, success) {
					expect(success[userMetadataKey]).to.equal(userMetadataValue);
					expect(success['Etag']).to.equal(correctMD5);
					done();
				})
			})
		})		
	});

});

describe("objectGet API",function(){

	let metastore;
	let datastore;

	beforeEach(function () {
	   metastore = {
			  "users": {
			      "accessKey1": {
			        "buckets": []
			      },
			      "accessKey2": {
			        "buckets": []
			      }
			  },
			  "buckets": {}
			};
		datastore = {};
	});

	const bucketName = 'BucketName';
	const postBody = 'I am a body';
	const correctMD5 = 'be747eb4b75517bf6b3cf7c5fbb62f3a';
	const incorrectMD5 = 'fkjwelfjlslfksdfsdfsdfsdfsdfsdj';
	const objectName = 'objectName';
	let date = new Date();
	let laterDate = date.setMinutes(date.getMinutes() + 30);
	let earlierDate = date.setMinutes(date.getMinutes() - 30);
	const testPutBucketRequest = {
		lowerCaseHeaders: {},
		url: `/${bucketName}`,
		namespace: namespace,
	};
	const userMetadataKey = 'x-amz-meta-test';
	const userMetadataValue = 'some metadata';
	const testPutObjectRequest = {
		lowerCaseHeaders: {
			'x-amz-meta-test': 'some metadata'
		},
		url: `/${bucketName}/${objectName}`,
		namespace: namespace,
		post: postBody,
		calculatedMD5: 'be747eb4b75517bf6b3cf7c5fbb62f3a'
	};

	it("should get the object metadata", function(done){
		const testGetRequest = {
			lowerCaseHeaders: {},
			url: `/${bucketName}/${objectName}`,
			namespace: namespace
		};

		bucketPut(accessKey, metastore, testPutBucketRequest, function(err, success) {
			expect(success).to.equal('Bucket created');
			objectPut(accessKey, datastore, metastore, testPutObjectRequest, function(err, result) {
				expect(result).to.equal(correctMD5);
				objectGet(accessKey, datastore, metastore, testGetRequest, function(err, result, responseMetaHeaders) {
					expect(responseMetaHeaders[userMetadataKey]).to.equal(userMetadataValue);
					expect(responseMetaHeaders['Etag']).to.equal(correctMD5);
					done();
				})
			})
		})		
	});

	it("should get the object data", function(done){
		const testGetRequest = {
			lowerCaseHeaders: {},
			url: `/${bucketName}/${objectName}`,
			namespace: namespace
		};

		bucketPut(accessKey, metastore, testPutBucketRequest, function(err, success) {
			expect(success).to.equal('Bucket created');
			objectPut(accessKey, datastore, metastore, testPutObjectRequest, function(err, result) {
				expect(result).to.equal(correctMD5);
				objectGet(accessKey, datastore, metastore, testGetRequest, function(err, result, responseMetaHeaders) {
					expect(result).to.equal(postBody);
					done();
				})
			})
		})		
	});


	it("should get the object data for large objects", function(done){
		const testBigData = crypto.randomBytes(1000000);
		const correctBigMD5 = crypto.createHash('md5').update(testBigData).digest('hex');

		const testPutBigObjectRequest = {
			lowerCaseHeaders: {
				'x-amz-meta-test': 'some metadata'
			},
			url: `/${bucketName}/${objectName}`,
			namespace: namespace,
			post: testBigData,
			calculatedMD5: correctBigMD5
		};

		const testGetRequest = {
			lowerCaseHeaders: {},
			url: `/${bucketName}/${objectName}`,
			namespace: namespace
		};

		bucketPut(accessKey, metastore, testPutBucketRequest, function(err, success) {
			expect(success).to.equal('Bucket created');
			objectPut(accessKey, datastore, metastore, testPutBigObjectRequest, function(err, result) {
				expect(result).to.equal(correctBigMD5);
				objectGet(accessKey, datastore, metastore, testGetRequest, function(err, result, responseMetaHeaders) {
					let resultmd5Hash = crypto.createHash('md5').update(result).digest('hex');
					expect(resultmd5Hash).to.equal(correctBigMD5);
					done();
				})
			})
		})		
	});
});


describe("bucketGet API",function(){

	let metastore;
	let datastore;

	beforeEach(function () {
	   metastore = {
			  "users": {
			      "accessKey1": {
			        "buckets": []
			      },
			      "accessKey2": {
			        "buckets": []
			      }
			  },
			  "buckets": {}
			};
		datastore = {};
	});

	const bucketName = 'BucketName';
	const postBody = 'I am a body';
	const prefix = 'sub';
	const delimiter = '/';
	const objectName1 = `${prefix}${delimiter}objectName1`;
	const objectName2 = `${prefix}${delimiter}objectName2`;

	const testPutBucketRequest = {
		lowerCaseHeaders: {},
		url: `/${bucketName}`,
		namespace: namespace,
	};
	const testPutObjectRequest1 = {
		lowerCaseHeaders: {},
		url: `/${bucketName}/${objectName1}`,
		namespace: namespace,
		post: postBody,
	};
	const testPutObjectRequest2 = {
		lowerCaseHeaders: {},
		url: `/${bucketName}/${objectName2}`,
		namespace: namespace,
		post: postBody
	};

	it("should return the name of the common prefix of common prefix objects \
		if delimiter and prefix specified", function(done){
		const commonPrefix = `${prefix}${delimiter}`;
		const testGetRequest = {
			lowerCaseHeaders: {
				host: '/'
			},
			url: `/${bucketName}?delimiter=\/&prefix=sub`,
			namespace: namespace,
			query: {
				delimiter: delimiter,
				prefix: prefix
			}
		};


		bucketPut(accessKey, metastore, testPutBucketRequest, function(err, success) {
			expect(success).to.equal('Bucket created');
			objectPut(accessKey, datastore, metastore, testPutObjectRequest1, function(err, result) {
				objectPut(accessKey, datastore, metastore, testPutObjectRequest2, function(err, result) {
					bucketGet(accessKey, metastore, testGetRequest, function(err, result) {
						parseString(result, function (err, result){
							expect(result.ListBucketResult.CommonPrefixes[0].Prefix[0]).to.equal(commonPrefix);
							done()
						})
					})
				})
			})
		})		
	});

	it("should return list of all objects if no delimiter specified", function(done){
		const testGetRequest = {
			lowerCaseHeaders: {
				host: '/'
			},
			url: `/${bucketName}`,
			namespace: namespace,
			query: {}
		};

		bucketPut(accessKey, metastore, testPutBucketRequest, function(err, success) {
			expect(success).to.equal('Bucket created');
			objectPut(accessKey, datastore, metastore, testPutObjectRequest1, function(err, result) {
				objectPut(accessKey, datastore, metastore, testPutObjectRequest2, function(err, result) {
					bucketGet(accessKey, metastore, testGetRequest, function(err, result) {
						parseString(result, function (err, result){
							expect(result.ListBucketResult.Contents[0].Key[0]).to.equal(objectName1);
							expect(result.ListBucketResult.Contents[1].Key[0]).to.equal(objectName2);
							done()
						})
					})
				})
			})
		})				
	});
});



describe("serviceGet API",function(){
	let metastore;

	beforeEach(function () {
	   metastore = {
			  "users": {
			      "accessKey1": {
			        "buckets": []
			      },
			      "accessKey2": {
			        "buckets": []
			      }
			  },
			  "buckets": {}
			}
	});

	it("should return the list of buckets owned by the user", function(done){

		const bucketName1 = 'BucketName1';
		const bucketName2 = 'BucketName2';
		const bucketName3 = 'BucketName3';
		const testbucketPutRequest1 = {
			lowerCaseHeaders: {},
			 url: '/',
			 namespace: namespace,
			 headers: {host: `${bucketName1}.s3.amazonaws.com`}
		};
		const testbucketPutRequest2 = {
			lowerCaseHeaders: {},
			 url: '/',
			 namespace: namespace,
			 headers: {host: `${bucketName2}.s3.amazonaws.com`}
		};
		const testbucketPutRequest3 = {
			lowerCaseHeaders: {},
			 url: '/',
			 namespace: namespace,
			 headers: {host: `${bucketName3}.s3.amazonaws.com`}
		};
		const serviceGetRequest = {
			lowerCaseHeaders: {host: 's3.amazonaws.com'},
			url: '/',
		};

		bucketPut(accessKey, metastore, testbucketPutRequest1, function(err, success) {
			bucketPut(accessKey, metastore, testbucketPutRequest2, function(err, success) {
				bucketPut(accessKey, metastore, testbucketPutRequest3, function(err, success) {
					serviceGet(accessKey, metastore, serviceGetRequest, function(err, result) {
						parseString(result, function (err, result){
							expect(result.ListAllMyBucketsResult.Buckets[0].Bucket).to.have.length.of(3);
							expect(result.ListAllMyBucketsResult.Buckets[0].Bucket[0].Name[0]).to.equal(bucketName1);
							expect(result.ListAllMyBucketsResult.Buckets[0].Bucket[1].Name[0]).to.equal(bucketName2);
							expect(result.ListAllMyBucketsResult.Buckets[0].Bucket[2].Name[0]).to.equal(bucketName3);
							done()
						});
					});
				});
			});
		});
	});
});









