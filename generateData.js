//generateData.js
'use strict';
const { MongoClient, ObjectId } = require('mongodb');
const faker = require('faker');
const passwordHash = require('password-hash');
const config = require('./config.js');

const MONGO_URI = config.cosmosDB.connectionString;
const DB_NAME = config.cosmosDB.authDBName || "AuthDB";
const NUM_USERS = 100;
const NUM_SCHOOLS_PER_USER = 10;
const BATCH_SIZE = 1;
const DELETE_BATCH_SIZE = 1
const RETRY_ATTEMPTS = 5;
const INITIAL_RETRY_DELAY = 2000;
const BASE_DELAY = 100;
let currentDelay = BASE_DELAY;

faker.locale = 'uk';


function getHashedPassword(plainPassword) {
  const options = {
    algorithm: 'sha256',
    saltLength: 128 / 8,
    iterations: 1000,
  };
  return passwordHash.generate(plainPassword, options);
}

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function insertWithRetry(collection, data, options = {}, attempt = 1) {
    try {
        const result = await collection.insertMany(data, { ordered: false, ...options });
        
        currentDelay = Math.max(BASE_DELAY, currentDelay - 5); 
        
        return result.insertedIds;
    } catch (err) {
        if (err.code === 16500 && attempt < RETRY_ATTEMPTS) {
            const delay = INITIAL_RETRY_DELAY * Math.pow(2, attempt - 1);
            console.warn(
                `Помилка швидкості запитів при вставці. Очікування ${delay}мс перед повторною спробою... (Спроба ${attempt})`
            );
            await sleep(delay);
            
            currentDelay += 100;
            
            const errorIndexes = err.writeErrors.map(error => error.err.index);
            const failedInserts = data.filter((_, index) => errorIndexes.includes(index));

            return insertWithRetry(collection, failedInserts, options, attempt + 1);
        } else {
            console.error('Помилка при вставці:', err);
            throw err;
        }
    }
}

async function deleteWithRetry(collection, filter, options = {}, attempt = 1) {
  try {
    const cursor = collection.find(filter).limit(DELETE_BATCH_SIZE);
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
      console.warn(
        `Помилка швидкості запитів при видаленні. Очікування ${delay}мс перед повторною спробою... (Спроба ${attempt})`
      );
      await sleep(delay);
      return deleteWithRetry(collection, filter, options, attempt + 1);
    } else {
      console.error('Помилка при видаленні:', err);
      throw err;
    }
  }
}

async function generateData() {
  const client = new MongoClient(MONGO_URI);
  try {
    await client.connect({ useNewUrlParser: true, useUnifiedTopology: true });
    console.log('Підключено до MongoDB');

    const db = client.db(DB_NAME);
    const usersCollection = db.collection('users');
    const schoolsCollection = db.collection('schools');

    console.log('Видалення існуючих користувачів...');
    await deleteWithRetry(usersCollection, {});

    console.log('Видалення існуючих шкіл...');
    await deleteWithRetry(schoolsCollection, {});

    console.log('Існуючих користувачів та школи видалено.');

    const adminUsers = [];
    for (let i = 0; i < NUM_USERS; i++) {
      const username = faker.internet.userName();
      const password = faker.internet.password();
      const hashedPassword = getHashedPassword(password);

      const user = {
        UserName: username,
        Password: hashedPassword,
        Role: 'Admin',
        schools: [],
      };
      adminUsers.push(user);

      await insertWithRetry(usersCollection, [user]);
      console.log('Додано користувача:', user.UserName);
    }
    console.log(`Додано ${adminUsers.length} користувачів-адміністраторів`);

    const schools = [];
    for (let i = 0; i < NUM_USERS * NUM_SCHOOLS_PER_USER; i++) {
      const school = {
        SchoolName: faker.company.companyName(),
        SchoolId: faker.datatype.number({ min: 10000, max: 99999 }),
        DeanName: faker.name.findName(),
        Email: faker.internet.email(),
      };
      schools.push(school)
      await insertWithRetry(schoolsCollection, [school]);
      console.log('Додано школу:', school.SchoolName);
      
      await sleep(currentDelay)
    }

    const insertedSchools = await schoolsCollection.find().toArray();

    const users = await usersCollection.find().toArray();

    if (insertedSchools.length > 0) {
      for (let i = 0; i < users.length; i++) {
        const user = users[i];
        const schoolIds = [];
        const startIndex = i * NUM_SCHOOLS_PER_USER;
        const endIndex = Math.min(startIndex + NUM_SCHOOLS_PER_USER, insertedSchools.length);
        for (let j = startIndex; j < endIndex; j++) {
          schoolIds.push(insertedSchools[j]._id);
        }
        await usersCollection.updateOne(
          { _id: user._id },
          { $set: { schools: schoolIds } }
        );
      }
      console.log('Зв\'язування користувачів і шкіл завершено!');
    } else {
      console.error('Помилка: Немає згенерованих шкіл для зв\'язування.');
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