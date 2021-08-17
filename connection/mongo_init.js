const MongoClient = require("mongodb").MongoClient;
const ObjectId = require("mongodb").ObjectID;

const MongoOption = {
  // promiseLibrary: Promise,
  useUnifiedTopology: true,
};

const host = process.env.MONGO_URL ? process.env.MONGO_URL : "localhost:27017";
const url = `mongodb://${host}`;

let client = new MongoClient(url, MongoOption);

async function initializeClient() {
  await client.connect();
  console.log("Initialized Connection to mongoDB at", host);
  return true;
}

async function closeClient() {
  await client.close();
  console.log("Closing MongoDB Connection");
}

async function query(dbName, collectionName, criteria, sorting, limits, skips) {
  let sort = sorting ? sorting : {};
  let limit = limits ? limits : 100;
  let skip = skips ? skips : 0;
  console.debug(
    `Running Query on ${collectionName}`,
    `C-${JSON.stringify(criteria)}`,
    `SO-${JSON.stringify(sort)}`,
    `L-${JSON.stringify(limit)}`,
    `SK-${JSON.stringify(skip)}`
  );

  try {
    const dbo = client.db(dbName);

    const result = await dbo
      .collection(collectionName)
      .find(criteria)
      .sort(sort)
      .limit(limit)
      .skip(skip)
      .toArray();
    return result;
  } catch (exception) {
    console.error(exception);
    return null;
  }
}

async function insertOne(dbName, collectionName, newObject) {
  try {
    const dbo = client.db(dbName);
    const result = await dbo.collection(collectionName).insertOne(newObject);
    return result;
  } catch (exception) {
    console.error(exception);
    return null;
  }
}

async function insertMany(dbName, collectionName, newObjectArray) {
  try {
    const dbo = client.db(dbName);
    const result = await dbo
      .collection(collectionName)
      .insertMany(newObjectArray);
    return result;
  } catch (exception) {
    console.error(exception);
    return null;
  }
}

async function findOneAndUpdate(
  dbName,
  collectionName,
  condition,
  updateObject
) {
  try {
    const dbo = client.db(dbName);
    const result = await dbo
      .collection(collectionName)
      .findOneAndUpdate(condition, updateObject, { upsert: true });
    return result;
  } catch (exception) {
    console.error(exception);
    throw exception;
  }
}

async function updateOne(dbName, collectionName, condition, updateObject) {
  try {
    const dbo = client.db(dbName);
    const result = await dbo
      .collection(collectionName)
      .updateOne(condition, { $set: updateObject }, { upsert: true });
    return result;
  } catch (exception) {
    console.error(exception);
    throw exception;
  }
}

async function removeOne(dbName, collectionName, condition) {
  try {
    const dbo = client.db(dbName);
    const result = await dbo.collection(collectionName).removeOne(condition);
    return result;
  } catch (exception) {
    console.error(exception);
    throw exception;
  }
}

async function removeMany(dbName, collectionName, condition) {
  try {
    const dbo = client.db(dbName);
    const result = await dbo.collection(collectionName).removeMany(condition);
    return result;
  } catch (exception) {
    console.error(exception);
    throw exception;
  }
}

function getObjectIdFromString(id) {
  return new ObjectId(id);
}

module.exports = {
  getClient: initializeClient,
  closeClient,
  query,
  insertOne,
  insertMany,
  findOneAndUpdate,
  updateOne,
  removeOne,
  removeMany,
  getObjectIdFromString,
};
