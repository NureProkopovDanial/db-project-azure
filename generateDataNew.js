'use strict';
const { MongoClient, ObjectId } = require('mongodb');
const faker = require('faker');
const config = require('./config.js');

const MONGO_URI = "mongodb://localhost:27017/sports_db";
const DB_NAME = "sports_db";
const NUM_SEASONS = 6;
const NUM_ATHLETES = 5000;
const NUM_RACES_PER_SEASON = 50;
// Оптимізовані налаштування
const BATCH_SIZE = 500;          // Збільшений розмір пакету
const RETRY_ATTEMPTS = 10;       // Максимальна кількість спроб
const INITIAL_RETRY_DELAY = 1000;// Початкова затримка у мілісекундахW
const DELETE_BATCH_SIZE = 1000;  // Розмір пакету для видалення
const BASE_DELAY = 500;          // Базова затримка між операціями

let currentDelay = BASE_DELAY;   // Динамічна затримка

faker.locale = 'uk';

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function insertWithRetry(collection, data, attempt = 1) {
    try {
        const result = await collection.insertMany(data, { ordered: false });
        currentDelay = Math.max(BASE_DELAY, currentDelay - 50);
        return result.insertedIds;

    } catch (err) {
        if (err.code === 16500 && attempt <= RETRY_ATTEMPTS) {
            const delay = INITIAL_RETRY_DELAY * Math.pow(2, attempt - 1);
            console.warn(`Помилка через ліміт запитів. Чекаємо ${delay}мс... (Спроба ${attempt})`);
            currentDelay += 500;
            await sleep(delay);

            const failedIndexes = new Set(err.writeErrors.map(e => e.index));
            const failedDocs = data.filter((_, index) => failedIndexes.has(index));
            return insertWithRetry(collection, failedDocs, attempt + 1);
        } 
        throw err;
    }
}

async function deleteWithRetry(collection) {
    try {
        let totalDeleted = 0;
        while (true) {
            const cursor = await collection.find().limit(DELETE_BATCH_SIZE).toArray();
            if (cursor.length === 0) break;

            const idsToDelete = cursor.map(doc => doc._id);
            const deleteResult = await collection.deleteMany({ 
                _id: { $in: idsToDelete } 
            });

            totalDeleted += deleteResult.deletedCount;
            console.log(`Видалено ${deleteResult.deletedCount} записів з ${collection.collectionName}`);
            await sleep(currentDelay);
        }
        return totalDeleted;
    } catch (err) {
        if (err.code === 16500) {
            console.warn(`Ліміт під час видалення. Повторна спроба...`);
            await sleep(INITIAL_RETRY_DELAY);
            return deleteWithRetry(collection);
        }
        throw err;
    }
}

// Головна функція генерації даних
async function generateData() {
    const client = new MongoClient(MONGO_URI, {
        serverSelectionTimeoutMS: 30000
    });
    
    try {
        await client.connect();
        console.log('Підключено до MongoDB');
        const db = client.db(DB_NAME);

        const collections = {
            seasons: db.collection('seasons'),
            athletes: db.collection('athletes'),
            races: db.collection('races'),
            results: db.collection('results')
        };

        console.log('Видаляємо старі дані...');
        for (const coll of Object.values(collections)) {
            await deleteWithRetry(coll);
        }

        const seasons = Array.from({length: NUM_SEASONS}, (_, i) => ({
            _id: new ObjectId(),
            year: 2022 + i,
            name: `Сезон ${2022 + i}-${2023 + i}`
        }));
        await insertWithRetry(collections.seasons, seasons);
        console.log(`✅ Додано ${seasons.length} сезонів`);

        const athletes = Array.from({length: NUM_ATHLETES}, () => ({
            _id: new ObjectId(),
            firstName: faker.name.firstName(),
            lastName: faker.name.lastName(),
            country: faker.address.country()
        }));
        await insertWithRetry(collections.athletes, athletes);
        console.log(`✅ Додано ${athletes.length} спортсменів`);

        const races = [];
        const results = [];
        const seasonIds = (await collections.seasons.find().toArray()).map(s => s._id);

        for (const seasonId of seasonIds) {
            for (let i = 0; i < NUM_RACES_PER_SEASON; i++) {
                const raceId = new ObjectId();
                races.push({
                    _id: raceId,
                    seasonId,
                    date: faker.date.between(
                        new Date(seasonId.getTimestamp().getFullYear(), 8, 1), 
                        new Date(seasonId.getTimestamp().getFullYear() + 1, 4, 1)
                    ),
                    type: faker.random.arrayElement([
                        "Роздільний старт", 
                        "Мас-старт", 
                        "Спринт", 
                        "Гонка переслідування"
                    ]),
                    location: faker.address.city()
                });

                const athletesSample = faker.helpers.shuffle(athletes).slice(0, 50);
                athletesSample.forEach((athlete, index) => {
                    results.push({
                        raceId,
                        athleteId: athlete._id,
                        place: index + 1
                    });
                });
            }
        }

        await insertWithRetry(collections.races, races);
        console.log(`✅ Додано ${races.length} перегонів`);

        let insertedResults = 0;
        while (insertedResults < results.length) {
            const batch = results.slice(insertedResults, insertedResults + BATCH_SIZE);
            await insertWithRetry(collections.results, batch);
            insertedResults += batch.length;
            console.log(`⏳ Прогрес: ${insertedResults}/${results.length} результатів`);
            await sleep(currentDelay);
        }
        console.log(`✅ Додано ${results.length} результатів`);

    } catch (err) {
        console.error('‼️ Критична помилка:', err);
    } finally {
        await client.close();
        console.log('Підключення закрито');
    }
}

generateData();
