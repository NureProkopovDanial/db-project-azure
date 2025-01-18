const { MongoClient } = require('mongodb');
const faker = require('faker');
const passwordHash = require('password-hash');

const MONGO_URI = config.cosmosDB.connectionString;
const AUTH_DB_NAME = config.cosmosDB.authDBName || "AuthDB";
const NUM_USERS = 100;
const NUM_SCHOOLS_PER_USER = 10;

faker.locale = 'uk_UA';

function getHashedPassword(plainPassword) {
    const options = {
        'algorithm': 'sha256',
        'saltLength': 128 / 8,
        'iterations': 1000
    };
    return passwordHash.generate(plainPassword, options);
}

async function generateData() {
    const client = new MongoClient(MONGO_URI);
    try {
        await client.connect({ useNewUrlParser: true, useUnifiedTopology: true });
        console.log("Підключено до MongoDB");

        const authDb = client.db(AUTH_DB_NAME);
        const usersCollection = authDb.collection("userlogin");

        const existingUsers = await usersCollection.find().toArray();
        for (const user of existingUsers) {
          const userDbName = user.DataBaseName;
          if (userDbName) {
            try {
              await client.db(userDbName).dropDatabase();
              console.log(`Базу даних ${userDbName} видалено.`);
            } catch (err) {
              console.error(`Помилка видалення бази даних ${userDbName}:`, err);
            }
          }
        }
        await usersCollection.deleteMany({});
        console.log("Існуючих користувачів видалено.");

        const adminUsers = [];
        for (let i = 0; i < NUM_USERS; i++) {
            const username = faker.internet.userName();
            const password = faker.internet.password();
            const hashedPassword = getHashedPassword(password);
            const dbName = `Keo_${username}`;
            const user = {
                "UserName": username,
                "Password": hashedPassword,
                "DataBaseName": dbName,
                "Role": "Admin",
                "organization": []
            };
            adminUsers.push(user);
        }

        await usersCollection.insertMany(adminUsers);
        console.log(`Додано ${NUM_USERS} користувачів-адміністраторів`);

        for (const adminUser of adminUsers) {
            const userDbName = adminUser.DataBaseName;
            const userDb = client.db(userDbName);
            const schoolsCollection = userDb.collection("school");

            const schools = [];
            for (let j = 0; j < NUM_SCHOOLS_PER_USER; j++) {
                const school = {
                    "SchoolName": faker.company.companyName(),
                    "SchoolId": faker.random.number({ min: 10000, max: 99999 }),
                    "DeanName": faker.name.findName(),
                    "Email": faker.internet.email()
                };
                schools.push(school);
            }

            await schoolsCollection.insertMany(schools);
            console.log(`Додано ${NUM_SCHOOLS_PER_USER} шкіл для користувача ${adminUser.UserName}`);
        }

        console.log("Генерація даних завершена!");
    } catch (err) {
        console.error("Помилка:", err);
    } finally {
        await client.close();
        console.log("Відключено від MongoDB");
    }
}

generateData();