const MongoClient = require("mongodb").MongoClient;
require('dotenv').config()


const MongoOption = {
  useUnifiedTopology: true,
};

async function replicate(
  sourceUrl,
  sourceDatabase,
  targetUrl,
  targetDatabase,
  collectionName,
  selectionCriteria
) {
  console.log("Replicate between database");

  return new Promise(async function (resolve, reject) {
    let sourceclient = initializeClient(sourceUrl, MongoOption);
    let targetclient = initializeClient(targetUrl, MongoOption);

    await Promise.all([sourceclient.connect(), targetclient.connect()]);
    console.log("Clients Ready");
    let sourceCursor = sourceclient
      .db(sourceDatabase)
      .collection(collectionName)
      .find(selectionCriteria)
      .stream();

    const ops = [];
    sourceCursor.on("data", async (data) => {
      console.log(`Processing Record ${data._id}`);
      ops.push(
        targetclient
          .db(targetDatabase)
          .collection(collectionName)
          .insertOne(data)
      );
    });

    sourceCursor.on("end", async (data) => {
      console.log("====================================================================================")
      console.log(
        "Replication is Completed from Source (",
        sourceDatabase,
        ") To (",
        targetDatabase,
        ") For",
        collectionName,
        "Matching criteria",
        selectionCriteria
      );
      console.log("Keep the console open and wait for the documents to be committed in the new database");
      console.log("====================================================================================")
      Promise.all(ops).then(async () => {
        await sourceclient.close();
        await targetclient.close();
        resolve({ status: "Completed" });
      });
    });
  });
}
function initializeClient(url, option) {
  return new MongoClient(url, option)
}

module.exports = {
  replicate,
};



// Mongo Altas woud need this 
// url = `mongodb+srv://${USERNAME}:${PASSWORD}@${host}?retryWrites=true&w=majority`;/

const sourceUrl = process.env.SOURCE_URL;
const targetUrl = process.env.TARGET_URL;
const sourceDatabase = process.env.SOURCE_DB_NAME;
const targetDatabase = process.env.TARGET_DB_NAME;
const collectionName = process.env.COLLECTION_TO_SYNC;

if (sourceUrl && targetUrl && sourceDatabase && targetDatabase && collectionName) {
  replicate(
    sourceUrl,
    sourceDatabase,
    targetUrl,
    targetDatabase,
    collectionName
  {}
  ).then((results) => {
    console.log(results);
  });

} else {
  console.error("Missing Parameters for Replication")
  process.exit(1);
}
