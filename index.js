import { fireproof } from '@fireproof/core/node';
import bent from 'bent';

const getJSON = bent('json');

async function initialData() {
    // 64 KB, 20 entries
    // return await getJSON('https://raw.githubusercontent.com/dstanesc/test-json-data/main/data/very-small-file.json')
    // 2.1 MB, 1000 entries
    // return await getJSON('https://raw.githubusercontent.com/dstanesc/test-json-data/main/data/small-file.json')
    // 9.3 MB, 4000 entries
    // return await getJSON('https://raw.githubusercontent.com/dstanesc/test-json-data/main/data/medium-file.json')
    // 26 MB, 11000 entries
    return await getJSON('https://raw.githubusercontent.com/dstanesc/test-json-data/main/data/large-file.json')
}

async function storeData(data) {
    const db = fireproof('playground-17-'+Date.now(), {autoCompact: 100000, public: true});

    console.log("Database initial state", db._crdt.clock.head.toString());

    console.time("all docs")

    const allDocs = await db.allDocs();

    console.log("Database allDocs", allDocs.rows.length);

    console.timeEnd("all docs")

    // let count = 0;

    const batchSize = 500;
    for (let i = 0; i < data.length; i += batchSize) {
        console.log(`log: ${i} - ${db._crdt.blockstore.loader.carLog.length}`)
        const batch = data.slice(i, i + batchSize);
        const ops = batch.map((item, j) => db.put(item));
        console.time("batch"+i)
        await Promise.all(ops);
        console.timeEnd("batch"+i)
    }

    return db;
}


async function queryData(db) {
    return await db.query("id", { descending: true, range: ['2400000000', '2900000000'], limit: 3 })
}

(async () => {
    console.time("fetch")
    const jsonData = await initialData();
    console.timeEnd("fetch")
    
    console.log("Fetched", jsonData.length, "entries");

    console.time("store")
    const db = await storeData(jsonData);
    console.timeEnd("store")

    console.time("query")
    const results2 = await queryData(db);
    console.timeEnd("query")

    console.log("Query results", results2.rows.length);
})();
