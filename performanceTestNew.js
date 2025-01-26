'use strict';
const { MongoClient } = require('mongodb');
const { performance } = require('perf_hooks');
const config = require('./config.js');

const MONGO_URI = "mongodb://localhost:27017/sports_db";
const DB_NAME = "sports_db";
const TEST_RUNS = 10;

async function runPerformanceTests(db) {
  // 1. Створюємо індекси для оптимізації
  await db.collection('results').createIndex({ raceId: 1 });
  await db.collection('races').createIndex({ type: 1, seasonId: 1 });
  await db.collection('seasons').createIndex({ year: 1 });
  await db.collection('athletes').createIndex({ country: 1 });

  // 2. Визначення запитів
  const queries = {
    query1: [
      { $match: { place: { $lte: 8 } } },
      { $lookup: { from: "races", localField: "raceId", foreignField: "_id", as: "race" } },
      { $unwind: "$race" },
      { $match: { "race.type": "Роздільний старт" } },
      { $lookup: { from: "seasons", localField: "race.seasonId", foreignField: "_id", as: "season" } },
      { $unwind: "$season" },
      { $lookup: { from: "athletes", localField: "athleteId", foreignField: "_id", as: "athlete" } },
      { $unwind: "$athlete" },
      { $group: {
          _id: { athleteId: "$athlete._id", seasonYear: "$season.year" },
          count: { $sum: 1 },
          lastName: { $first: "$athlete.lastName" },
          country: { $first: "$athlete.country" }
      }},
      { $match: { count: { $gte: 3 } } },
      { $sort: { "_id.seasonYear": 1 } },
      { $group: {
          _id: "$_id.athleteId",
          seasons: { $push: "$_id.seasonYear" },
          lastName: { $first: "$lastName" },
          country: { $first: "$country" }
      }},
      { $project: {
          consecutiveSeasons: {
            $reduce: {
              input: "$seasons",
              initialValue: { current: [], max: 0 },
              in: {
                current: {
                  $cond: [
                    { $eq: [{ $subtract: ["$$this", { $arrayElemAt: ["$$value.current", -1] }] }, 1] },
                    { $concatArrays: ["$$value.current", ["$$this"]] },
                    ["$$this"]
                  ]
                },
                max: {
                  $cond: [
                    { $gt: [{ $size: "$$value.current" }, "$$value.max"] },
                    { $size: "$$value.current" },
                    "$$value.max"
                  ]
                }
              }
            }
          },
          lastName: 1,
          country: 1
      }},
      { $match: { "consecutiveSeasons.max": { $gte: 3 } } }
    ],

    query2: [
      { $lookup: { from: "races", localField: "raceId", foreignField: "_id", as: "race" } },
      { $unwind: "$race" },
      { $match: { "race.type": "Мас-старт" } },
      { $lookup: { from: "seasons", localField: "race.seasonId", foreignField: "_id", as: "season" } },
      { $unwind: "$season" },
      { $match: { "season.year": { $gte: new Date().getFullYear() - 2 } } },
      { $group: {
          _id: "$race._id",
          avgPlace: { $avg: "$place" },
          date: { $first: "$race.date" },
          location: { $first: "$race.location" }
      }},
      { $sort: { avgPlace: 1 } },
      { $limit: 5 }
    ],

    query3: [
      { $match: { place: 1 } },
      { $lookup: { from: "races", localField: "raceId", foreignField: "_id", as: "race" } },
      { $unwind: "$race" },
      { $match: { "race.type": "Спринт" } },
      { $lookup: { from: "athletes", localField: "athleteId", foreignField: "_id", as: "athlete" } },
      { $unwind: "$athlete" },
      { $group: {
          _id: { country: "$athlete.country", athleteId: "$athlete._id" },
          wins: { $sum: 1 },
          firstName: { $first: "$athlete.firstName" },
          lastName: { $first: "$athlete.lastName" }
      }},
      { $sort: { "_id.country": 1, "wins": -1 } },
      { $group: {
          _id: "$_id.country",
          athletes: { $push: { name: { $concat: ["$firstName", " ", "$lastName"] }, wins: "$wins" } }
      }},
      { $project: {
          country: "$_id",
          topAthletes: { $slice: ["$athletes", 3] }
      }}
    ]
  };

  // 3. Визначення MapReduce-функцій
  const mapReduceFunctions = {
    query1: {
      map: function() {
        emit(
          { athleteId: this.athleteId, season: this.season },
          { count: this.place <= 8 ? 1 : 0 }
        );
      },
      reduce: function(key, values) {
        return { count: values.reduce((sum, v) => sum + v.count, 0) };
      },
      finalize: function(key, reduced) {
        return reduced.count >= 3 ? reduced : null;
      }
    },

    query2: {
      map: function() {
        emit(this.raceId, { sum: this.place, count: 1 });
      },
      reduce: function(key, values) {
        return {
          sum: values.reduce((acc, v) => acc + v.sum, 0),
          count: values.reduce((acc, v) => acc + v.count, 0)
        };
      },
      finalize: function(key, reduced) {
        return { avgPlace: reduced.sum / reduced.count };
      }
    },

    query3: {
      map: function() {
        if (this.place !== 1) return;

        emit(
          { country: this.country, athleteId: this.athleteId },
          { wins: 1, name: this.firstName + " " + this.lastName }
        );
      },
      reduce: function(key, values) {
        return {
          wins: values.reduce((acc, v) => acc + v.wins, 0),
          name: values[0].name
        };
      }
    }
  };

  // 4. Тестування продуктивності
  const results = {};

  for (const queryName of Object.keys(queries)) {
    console.log(`\n=== Тестування ${queryName} ===`);

    // Тестування звичайних запитів
    const regularTimes = [];
    for (let i = 0; i < TEST_RUNS; i++) {
      const start = performance.now();
      await db.collection('results').aggregate(queries[queryName]).toArray();
      regularTimes.push(performance.now() - start);
      console.log(`Regular ${queryName} run ${i+1}: ${regularTimes[i].toFixed(2)} ms`);
    }

    // Тестування MapReduce
    const mrTimes = [];
    const mrConfig = mapReduceFunctions[queryName];
    for (let i = 0; i < TEST_RUNS; i++) {
      const start = performance.now();
      await db.collection('results').mapReduce(
        mrConfig.map,
        mrConfig.reduce,
        {
          out: { inline: 1 },
          finalize: mrConfig.finalize
        }
      );
      mrTimes.push(performance.now() - start);
      console.log(`MapReduce ${queryName} run ${i+1}: ${mrTimes[i].toFixed(2)} ms`);
    }

    // Збереження результатів
    results[queryName] = {
      regular: calculateStats(regularTimes),
      mapReduce: calculateStats(mrTimes)
    };
  }

  // 5. Вивід результатів
  console.log("\n\n=== Фінальні результати ===");
  for (const [queryName, data] of Object.entries(results)) {
    console.log(`\nЗапит: ${queryName}`);
    printStats("Звичайний запит", data.regular);
    printStats("MapReduce", data.mapReduce);
  }
}

// Допоміжні функції
function calculateStats(times) {
  return {
    min: Math.min(...times).toFixed(2),
    max: Math.max(...times).toFixed(2),
    avg: (times.reduce((a, b) => a + b, 0) / times.length).toFixed(2)
  };
}

function printStats(label, stats) {
  console.log(`
  ${label}:
    Мінімальний: ${stats.min} мс
    Максимальний: ${stats.max} мс
    Середній: ${stats.avg} мс
  `);
}

async function main() {
    const client = new MongoClient(MONGO_URI, {
        serverSelectionTimeoutMS: 30000,
        useUnifiedTopology: true
    });

  try {
    await client.connect();
    console.log("Підключено до MongoDB Atlas");
    await runPerformanceTests(client.db(DB_NAME));
  } catch (err) {
    console.error("Помилка:", err);
  } finally {
    await client.close();
    console.log("Відключено від MongoDB");
  }
}

main();