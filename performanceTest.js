'use strict';
const { MongoClient, ObjectId } = require('mongodb');
const config = require('./config.js');

const MONGO_URI = config.cosmosDB.connectionString;
const DB_NAME = config.cosmosDB.authDBName || "AuthDB";

async function executeQueryWithTiming(collection, query, update = null, options = {}) {
  const startTime = performance.now();
  let result;
  if (update) {
    result = await collection.updateOne(query, update, options);
  } else {
    if (query.length) {
      result = await collection.aggregate(query).toArray();
    } else {
      result = await collection.findOne(query);
    }
  }
  const endTime = performance.now();
  const executionTime = endTime - startTime;
  return { result, executionTime };
}

async function runPerformanceTests(db) {
  const usersCollection = db.collection('users');
  const schoolsCollection = db.collection('schools');

  const userId = new ObjectId("678edb0e54b1a30756258d00");
  const schoolId = new ObjectId("678edb1054b1a30756258d63");

  const queries = [
    { name: "1. Отримати користувача за _id", collection: usersCollection, query: { _id: userId } },
    {
      name: "2. Отримати школи користувача", collection: usersCollection, query: [
        { $match: { _id: userId } },
        {
          $lookup: {
            from: "schools",
            localField: "schools",
            foreignField: "_id",
            as: "userSchools"
          }
        },
        { $unwind: "$userSchools" },
        { $replaceRoot: { newRoot: "$userSchools" } }
      ]
    },
    { name: "3. Отримати школу за _id", collection: schoolsCollection, query: { _id: schoolId } },
    { name: "4. Знайти школи з комою в назві", collection: schoolsCollection, query: [{ $match: { SchoolName: { $regex: "," } } }] },
    { name: "5. Оновити профіль користувача", collection: usersCollection, query: { _id: userId }, update: { $set: { Role: "UpdatedRole" } }, options: { returnDocument: 'after' } },
  ];

  for (const queryData of queries) {
    console.log(`\nВиконується запит: ${queryData.name}`);
    const executionTimes = [];
    for (let i = 0; i < 10; i++) {
      try {
        let result;
        let executionTime;
          const queryResult = await executeQueryWithTiming(queryData.collection, queryData.query, queryData.update, queryData.options);
          result = queryResult.result;
          executionTime = queryResult.executionTime;
        console.log(`  Час виконання (спроба ${i + 1}): ${executionTime.toFixed(2)} мс`);
        executionTimes.push(executionTime);
      } catch (err) {
        console.error(`  Помилка виконання запиту (спроба ${i + 1}):`, err);
      }
    }

    const minExecutionTime = Math.min(...executionTimes);
    const maxExecutionTime = Math.max(...executionTimes);
    const averageExecutionTime = executionTimes.reduce((sum, time) => sum + time, 0) / executionTimes.length;

    console.log(`Результати для ${queryData.name}:`);
    console.log(`  Мінімальний час виконання: ${minExecutionTime.toFixed(2)} мс`);
    console.log(`  Максимальний час виконання: ${maxExecutionTime.toFixed(2)} мс`);
    console.log(`  Середній час виконання: ${averageExecutionTime.toFixed(2)} мс`);
  }
}

async function main() {
  const client = new MongoClient(MONGO_URI, { useNewUrlParser: true, useUnifiedTopology: true });
  try {
    await client.connect();
    console.log('Підключено до MongoDB');
    const db = client.db(DB_NAME);

    await runPerformanceTests(db);

  } catch (err) {
    console.error('Помилка:', err);
  } finally {
    await client.close();
    console.log('Відключено від MongoDB');
  }
}

main();