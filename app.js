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
const pretty = require('pretty-time');
const ORDER_HEADERS = ['o_orderkey', 'o_custkey', 'o_orderstatus', 'o_totalprice', 'o_orderdate', 'o_orderpriority', 'o_clerk', 'o_shippriority', 'o_comment'];

function getStream(bucket, key) {
	var params = {
		Bucket: bucket,
		Key: key
	};
	return s3.getObject(params).createReadStream();
}

const BATCH_MAX = 500;


function createJson(headers, line) {
	let json = {};

	let items = line.split('|');

	for (let i = 0; i < headers.length; i++) {
		let item = items.length - 1 >= i ? items[i] : '';
		json[headers[i]] = item;
	}

	return JSON.stringify(json);
}

let count = 0;
var start = process.hrtime();

let elapsed_time = function (note, interval, reset) {
	if (!interval) {
		interval = 'ms';
	}

	console.log(`${pretty(process.hrtime(start), interval)}${note ? ' -- ' + note : ''}`); // print message + time

	if (reset) {
		start = process.hrtime(); // reset the timer
	}
}

function intervalFunc() {
	elapsed_time(`${count} processed`, 's');
}

setInterval(intervalFunc, 5000);

async function run() {
	let stream = getStream('jaaromy-emr-input', 'bigdatalab/emrdata/orders.tbl');

	eos(stream, function (err) {
		// this will be set to the stream instance
		if (err) return console.log('stream had an error or closed early');
		console.log('stream has ended', this === readableStream);
	});

	stream
		.pipe(es.split())
		.pipe(es.map((line, callback) => {
			callback(null, createJson(ORDER_HEADERS, line));
		}))
		.pipe(batch(BATCH_MAX))
		.pipe(es.map(async (records, callback) => {
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

			let dz = await kinesis.putRecords(params).promise();

			callback(null, dz.Records.length);
		}))
		.pipe(batch(20))
		.pipe(es.mapSync((records) => {
			records.map(val => {
				count += val;
			});
		}));
}

run();
