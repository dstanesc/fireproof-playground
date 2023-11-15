import { fireproof, Index } from '@fireproof/core/node';
import bent from 'bent';

const getJSON = bent('json');

async function initialData() {
    // 64 KB, 20 entries
    // return await getJSON('https://raw.githubusercontent.com/dstanesc/test-json-data/main/data/very-small-file.json')
    // 2.1 MB, 1000 entries
    // return await getJSON('https://raw.githubusercontent.com/dstanesc/test-json-data/main/data/small-file.json')
    // 9.3 MB, 4000 entries
    return await getJSON('https://raw.githubusercontent.com/dstanesc/test-json-data/main/data/medium-file.json')
    // 26 MB, 11000 entries
    // return await getJSON('https://raw.githubusercontent.com/dstanesc/test-json-data/main/data/large-file.json')
}

async function storeData(data) {
    const db = fireproof('playground', { public: true });
    for (const item of data) {
        await db.put(item);
    }
    return db;
}

async function indexData(db) {
    const index = new Index(db._crdt, 'event_id', (doc) => {
        return doc.id;
    })
    return index
}

async function queryIndex(index) {
    // type QueryOpts = {
    //     descending?: boolean
    //     limit?: number
    //     includeDocs?: boolean
    //     range?: [IndexKey, IndexKey]
    //     key?: DocFragment,
    //     keys?: DocFragment[]
    //     prefix?: DocFragment | [DocFragment]
    //   }
    // type IndexKey = [string, string] | string
    // { key: '2489651045'}
    const results = await index.query({ includeDocs: false, descending: true, range: ['2400000000', '2900000000'], limit: 3 });
    return results;
}

async function queryData(db) {
    return await db.query("id", { descending: true, range: ['2400000000', '2900000000'], limit: 3 })
}

(async () => {
    const start = Date.now();
    const jsonData = await initialData();
    const end = Date.now();
    console.log("Fetched", jsonData.length, "entries in", end - start, " ms");
    const db = await storeData(jsonData);
    const end2 = Date.now();
    console.log("Data stored in the database in", end2 - end, " ms");
    const index = await indexData(db);
    const end3 = Date.now();
    console.log("Data indexed in ", end3 - end2, "ms");
    const results = await queryIndex(index);
    const end4 = Date.now();
    console.log("Index queried in", end4 - end3, "ms");
    results.rows.forEach(row => {
        // console.log(JSON.stringify(row, null, 2));
    });
    const results2 = await queryData(db);
    const end5 = Date.now();
    console.log("Database queried in", end5 - end4, "ms");
    results2.rows.forEach(row => {
        // console.log(JSON.stringify(row, null, 2));
    });
})();
