const AWS = require('aws-sdk');
const Promise = require('bluebird');
AWS.config.setPromisesDependency(Promise);
const s3 = new AWS.S3({
	region: 'us-east-1'
});
const kinesis = new AWS.Kinesis({
	region: 'us-west-2'
});
const es = require('event-stream');
const eos = require('end-of-stream');
const batch = require('through-batch');
const ORDER_HEADERS = ['o_orderkey', 'o_custkey', 'o_orderstatus', 'o_totalprice', 'o_orderdate', 'o_orderpriority', 'o_clerk', 'o_shippriority', 'o_comment'];

function getStream(bucket, key) {
	var params = {
		Bucket: bucket,
		Key: key
	};
	return s3.getObject(params).createReadStream();
}

const BATCH_MAX = 400;


function createJson(headers, line) {
	let json = {};

	let items = line.split('|');

	for (let i = 0; i < headers.length; i++) {
		let item = items.length - 1 >= i ? items[i] : '';
		json[headers[i]] = item;
	}

	return JSON.stringify(json);
}

function run() {
	let stream = getStream('jaaromy-emr-input', 'bigdatalab/emrdata/orders.tbl');
	// converter
	// 	.fromStream(stream)
	// 	.subscribe(json => {
	// 		return Promise.try(() => {
	// 			console.log(json);
	// 		});
	// 	});

	eos(stream, function (err) {
		// this will be set to the stream instance
		if (err) return console.log('stream had an error or closed early');
		console.log('stream has ended', this === readableStream);
	});

	let count = 0;
	stream
		.pipe(es.split())
		.pipe(es.map((line, callback) => {
			callback(null, createJson(ORDER_HEADERS, line));
		}))
		.pipe(batch(BATCH_MAX))
		.pipe(es.mapSync((records, callback) => {
			let params = {
				Records: [],
				StreamName: 'jaaromy-stream-1'
			};

			for (let i = 0; i < records.length; i++) {
				let data = records[i];

				params.Records.push({
					Data: data,
					PartitionKey: 'SomeKey'
				});
			}

			stream.pause();
			count += records.length;
			if (count % 10000 === 0) {
				console.log(count);
			}
			kinesis.putRecords(params).promise()
				.then(data => {
					// console.log(JSON.stringify(data, null, 2));
					return data;
				})
				.catch(err => {
					console.error(err);
					throw err;
				})
				.finally(() => {
					stream.resume();
				});
		}));
}

run();
