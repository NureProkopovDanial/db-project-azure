'use strict';
var configObject = new Object();
configObject.issuer = "Keo plus LMS",
configObject.secret = 'Torches for tomorrow'; //Secret key for signing JWT.
configObject.jwtExpiresOn = 86400; //Time in seconds (24 hours)
configObject.jwtExpiredAt = 1; //no.of days
configObject.Prefix = 'Keo_'; //no.of days

// Обновленные настройки для подключения к Cosmos DB
configObject.cosmosDB = {
    connectionString: "mongodb://dpdbaccount:ymAOhAWxfczxoteCSm9tXx7EzYG6MSs3yV52iKNRSf7YX8s9ru1jkAJn4cph0KtplfD7Wf5E0MGiACDbvszxxA==@dpdbaccount.mongo.cosmos.azure.com:10255/?ssl=true&retrywrites=false&maxIdleTimeMS=120000&appName=@dpdbaccount@",
    authDBName: "AuthDB" // Имя базы данных для аутентификации (если отличается)
};

// Настройки Redis (если используете облачный Redis)
configObject.redisDB = {
    DBHOST: 'YOUR_REDIS_HOST', // Замените на адрес вашего Redis
    DBPORT: 'YOUR_REDIS_PORT', // Замените на порт вашего Redis
    USERNAME: 'YOUR_REDIS_USERNAME', // Замените на имя пользователя Redis
    PASSWORD: 'YOUR_REDIS_PASSWORD' // Замените на пароль Redis
};

module.exports = configObject;

