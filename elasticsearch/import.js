//const elasticsearch = require('elasticsearch');
const csv = require('csv-parser');
const fs = require('fs');
const { Client } = require('@elastic/elasticsearch');

const ELASTIC_SEARCH_URI = 'http://localhost:9200';
const INDEX_NAME = '911-calls';

async function run() {
  const client = new Client({ node: ELASTIC_SEARCH_URI});

  // Drop index if exists
  await client.indices.delete({
    index: INDEX_NAME,
    ignore_unavailable: true
  });

  await client.indices.create({
    index: INDEX_NAME,
    body : {
      "mappings": {
		"properties": {
		  "location": { "type": "geo_point" },
		  "timeStamp": { "type": "date" }
		}
	  }
    }
  });
  calls = [];	
  fs.createReadStream('../911.csv')
    .pipe(csv())
    .on('data', data => {
      const call = { 
		location : { "lat" : data.lat, "lon": data.lng},
		desc : data.desc,
		zip : data.zip,
		title : data.title,
		type : data.title.split(":")[0],
		timeStamp : new Date(data.timeStamp),
		twp : data.twp,
		addr : data.addr
	  };
    calls.push(call);
    })
    .on('end', async () => {
      client.bulk(createBulkInsertQuery(calls), (err, resp) => {
      if (err) console.trace(err);
      else console.log(`Inserted ${resp.body.items.length} calls`);
      client.close();
		});
	});
   function createBulkInsertQuery(calls) {
	  const body = calls.reduce((acc, call) => {
		const { location, desc, zip, title, type, timeStamp, twp, addr } = call;
		acc.push({ index: { _index: '911-calls', _type: '_doc' } })
		acc.push({ location, desc, zip, title, type, timeStamp, twp, addr })
		return acc
	  }, []);

	  return { body };
   }

}

run().catch(console.log);


