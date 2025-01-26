// generateData.js
'use strict';
const { MongoClient, ObjectId } = require('mongodb');
const faker = require('faker');
const passwordHash = require('password-hash');
const config = require('./config.js');

const MONGO_URI = config.cosmosDB.connectionString;
const DB_NAME = config.cosmosDB.authDBName || "AuthDB";

// 🔧 Оптимізовані налаштування
const NUM_USERS = 1000;
const NUM_SCHOOLS_PER_USER = 10;
const BATCH_SIZE = 250;                  // 🚀 Збільшений розмір пакету x250
const DELETE_BATCH_SIZE = 500;           // 🗑️ Ефективне видалення
const RETRY_ATTEMPTS = 8;                // 🔁 Більше спроб
const INITIAL_RETRY_DELAY = 1500;        // ⏳ Розумні затримки
const BASE_DELAY = 300;

let currentDelay = BASE_DELAY;

faker.locale = 'uk';

function getHashedPassword(plainPassword) {
  return passwordHash.generate(plainPassword, {
    algorithm: 'sha256',
    saltLength: 16,
    iterations: 1000
  });
}

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

// 🌟 Універсальна функція для вставки з ретраями
async function insertWithRetry(collection, data, attempt = 1) {
    try {
        const result = await collection.insertMany(data, { ordered: false });
        
        // 🎚️ Адаптивна регулювання затримки
        currentDelay = Math.max(BASE_DELAY, currentDelay - 25);
        return result.insertedIds;

    } catch (err) {
        if (err.code === 16500 && attempt <= RETRY_ATTEMPTS) {
            const delay = INITIAL_RETRY_DELAY * Math.pow(2, attempt - 1);
            console.warn(`⚠️ Rate limit! Waiting ${delay}ms... (Attempt ${attempt})`);
            
            currentDelay += 200;
            await sleep(delay);

            // 🎯 Ідентифікація невдалих документів
            const failedIndexes = new Set(err.writeErrors.map(e => e.index));
            const failedDocs = data.filter((_, i) => failedIndexes.has(i));

            return insertWithRetry(collection, failedDocs, attempt + 1);
        } 
        throw err;
    }
}

// 🧹 Ефективне видалення даних
async function deleteWithRetry(collection) {
    try {
        let totalDeleted = 0;
        while (true) {
            const docs = await collection.find().limit(DELETE_BATCH_SIZE).toArray();
            if (!docs.length) break;
            
            const ids = docs.map(d => d._id);
            const { deletedCount } = await collection.deleteMany({ _id: { $in: ids } });
            
            totalDeleted += deletedCount;
            console.log(`♻️ Deleted ${deletedCount} from ${collection.collectionName}`);
            
            await sleep(currentDelay);
        }
        return totalDeleted;
    } catch (err) {
        if (err.code === 16500) {
            console.warn(`⚠️ Delete rate limit! Retrying...`);
            await sleep(INITIAL_RETRY_DELAY);
            return deleteWithRetry(collection);
        }
        throw err;
    }
}

async function generateData() {
    const client = new MongoClient(MONGO_URI);
    
    try {
        await client.connect();
        console.log('🔌 Підключено до MongoDB');
        
        const db = client.db(DB_NAME);
        const [usersCollection, schoolsCollection] = await Promise.all([
            db.collection('users'),
            db.collection('schools')
        ]);

        // 🧼 Очищення старих даних
        console.log('\n🧹 Початок очищення бази...');
        const [deletedUsers, deletedSchools] = await Promise.all([
            deleteWithRetry(usersCollection),
            deleteWithRetry(schoolsCollection)
        ]);
        console.log(`\n✅ Видалено: 
        - ${deletedUsers} користувачів
        - ${deletedSchools} шкіл`);

        // 👥 Генерація користувачів
        console.log('\n👥 Початок генерації користувачів...');
        const users = Array.from({length: NUM_USERS}, () => ({
            UserName: faker.internet.userName(),
            Password: getHashedPassword(faker.internet.password()),
            Role: 'Admin',
            schools: []
        }));

        // 🚀 Пакетна вставка користувачів
        let insertedUsers = 0;
        while (insertedUsers < users.length) {
            const batch = users.slice(insertedUsers, insertedUsers + BATCH_SIZE);
            await insertWithRetry(usersCollection, batch);
            insertedUsers += batch.length;
            console.log(`📦 Вставлено ${insertedUsers}/${users.length} користувачів`);
            await sleep(currentDelay);
        }

        // 🏫 Генерація шкіл
        console.log('\n🏫 Початок генерації шкіл...');
        const totalSchools = NUM_USERS * NUM_SCHOOLS_PER_USER;
        const schools = Array.from({length: totalSchools}, () => ({
            SchoolName: faker.company.companyName(),
            SchoolId: faker.datatype.number({ min: 10000, max: 99999 }),
            DeanName: faker.name.findName(),
            Email: faker.internet.email()
        }));

        // 📦 Пакетна вставка шкіл
        let insertedSchools = 0;
        while (insertedSchools < schools.length) {
            const batch = schools.slice(insertedSchools, insertedSchools + BATCH_SIZE);
            await insertWithRetry(schoolsCollection, batch);
            insertedSchools += batch.length;
            console.log(`📚 Вставлено ${insertedSchools}/${schools.length} шкіл`);
            await sleep(currentDelay);
        }

        // 🔗 Зв'язування користувачів з школами
        console.log('\n🔗 Початок зв\'язування...');
        const allSchools = await schoolsCollection.find().toArray();
        const updateOperations = [];
        
        for (let i = 0; i < users.length; i++) {
            const startIdx = i * NUM_SCHOOLS_PER_USER;
            const endIdx = startIdx + NUM_SCHOOLS_PER_USER;
            const schoolIds = allSchools
                .slice(startIdx, endIdx)
                .map(s => s._id);

            updateOperations.push({
                updateOne: {
                    filter: { _id: users[i]._id },
                    update: { $set: { schools: schoolIds } }
                }
            });

            // Пакетне оновлення кожні 500 операцій
            if (updateOperations.length === 500) {
                await usersCollection.bulkWrite(updateOperations);
                updateOperations.length = 0;
                console.log(`🔁 Оновлено ${i + 1}/${users.length} користувачів`);
            }
        }

        // Фінальне оновлення залишкових операцій
        if (updateOperations.length > 0) {
            await usersCollection.bulkWrite(updateOperations);
            console.log(`🎉 Оновлено всіх ${users.length} користувачів!`);
        }

        console.log('\n✅ Генерація даних успішно завершена!');
    } catch (err) {
        console.error('\n‼️ Критична помилка:', err);
    } finally {
        await client.close();
        console.log('\n🔌 Відключено від MongoDB');
    }
}

generateData();