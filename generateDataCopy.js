'use strict';
const { MongoClient, ObjectId } = require('mongodb');
const config = require('./config.js');

const MONGO_URI = config.cosmosDB.connectionString;
const DB_NAME = config.cosmosDB.authDBName || "AuthDB";
const RETRY_ATTEMPTS = 5;
const INITIAL_RETRY_DELAY = 2000;

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function deleteWithRetry(collection, filter, options = {}, attempt = 1) {
    try {
        const cursor = collection.find(filter).limit(100);
        let deletedCount = 0;
        while (await cursor.hasNext()) {
            const doc = await cursor.next();
            await collection.deleteOne({ _id: doc._id });
            deletedCount++;
        }
        console.log(`Видалено ${deletedCount} документів з колекції ${collection.collectionName}.`);
    } catch (err) {
        if (err.code === 16500 && attempt < RETRY_ATTEMPTS) {
            const delay = INITIAL_RETRY_DELAY * Math.pow(2, attempt - 1);
            console.warn(`Помилка швидкості запитів при видаленні. Очікування ${delay}мс перед повторною спробою... (Спроба ${attempt})`);
            await sleep(delay);
            return deleteWithRetry(collection, filter, options, attempt + 1);
        } else {
            console.error('Помилка при видаленні:', err);
            throw err;
        }
    }
}

async function copyData(sourceCollection, destinationCollection) {
    const batchSize = 100;
    let skip = 0;
    let hasMore = true;

    while (hasMore) {
        const cursor = sourceCollection.find({}).skip(skip).limit(batchSize);
        const documents = await cursor.toArray();

        if (documents.length > 0) {
            const documentsToInsert = documents.map(doc => {
                delete doc._id;
                return doc;
            });

            try {
                await destinationCollection.insertMany(documentsToInsert, { ordered: false });
                console.log(`Успішно перенесено ${documents.length} документів з ${sourceCollection.collectionName} до ${destinationCollection.collectionName}`);
            } catch (err) {
                if (err.code === 16500) {
                    console.warn(`Помилка швидкості запитів при вставці. Очікування...`);
                    await sleep(INITIAL_RETRY_DELAY);
                    continue;
                } else {
                    console.error(`Помилка при перенесенні документів з ${sourceCollection.collectionName} до ${destinationCollection.collectionName}:`, err);
                    throw err;
                }
            }
            skip += documents.length;
        } else {
            hasMore = false;
        }
    }
}

async function generateData() {
    const client = new MongoClient(MONGO_URI, { useNewUrlParser: true, useUnifiedTopology: true });
    try {
        await client.connect();
        console.log('Підключено до MongoDB');
        const db = client.db(DB_NAME);

        const newUsersCollection = db.collection('users_new');
        const newSchoolsCollection = db.collection('schools_new');

        console.log('Видалення існуючих користувачів з users_new...');
        await deleteWithRetry(newUsersCollection, {});
        console.log('Видалення існуючих шкіл з schools_new...');
        await deleteWithRetry(newSchoolsCollection, {});
        console.log('Існуючих користувачів та школи з _new колекцій видалено.');

        const oldUsersCount = await db.collection('users').countDocuments({});
        const oldSchoolsCount = await db.collection('schools').countDocuments({});

        if (oldUsersCount > 0) {
            console.log('Перенесення даних з колекції users в users_new...');
            await copyData(db.collection('users'), newUsersCollection);
        } else {
            console.log('Колекція users пуста, перенесення даних не потрібне.');
        }

        if (oldSchoolsCount > 0) {
            console.log('Перенесення даних з колекції schools в schools_new...');
            await copyData(db.collection('schools'), newSchoolsCollection);
        } else {
            console.log('Колекція schools пуста, перенесення даних не потрібне.');
        }

        const users = await newUsersCollection.find().toArray();
        const insertedSchools = await newSchoolsCollection.find().toArray();

        if (insertedSchools.length > 0 && users.length > 0) {
            for (let i = 0; i < users.length; i++) {
                const user = users[i];
                const schoolIds = [];
                const startIndex = i * 10;
                const endIndex = Math.min(startIndex + 10, insertedSchools.length);
                for (let j = startIndex; j < endIndex; j++) {
                    schoolIds.push(insertedSchools[j]._id);
                }
                await newUsersCollection.updateOne(
                    { _id: user._id },
                    { $set: { schools: schoolIds } }
                );
            }
            console.log('Зв\'язування користувачів і шкіл завершено!');
        } else {
            console.error('Помилка: Немає даних для зв\'язування користувачів і шкіл.');
        }

        console.log('Генерація даних завершена!');

    } catch (err) {
        console.error('Помилка:', err);
    } finally {
        await client.close();
        console.log('Відключено від MongoDB');
    }
}

generateData();