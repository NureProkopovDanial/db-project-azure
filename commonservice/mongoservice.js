//mongoservice.js
'use strict';
var mongoose = require('mongoose'),
    Admin = mongoose.mongo.Admin;

var dataBaseSchema = require('../models/school.model');
var config = require('../config.js')

/**
 * 1. Connect local Mongo server and get all Database 
 * 2. Match the DB name under username else create new DB
 * 3. Create Collection under new DB
 * @param {username} UserDbName 
 */
function createDB(UserDbName, cb) {
    var uri = config.cosmosDB.connectionString;
    var AdminDb = mongoose.createConnection(uri);
    AdminDb.on('open', function () {
        var dbExists;
        new Admin(AdminDb.db).listDatabases(function (err, result) {
            console.log('listDatabases succeeded');
            var allDatabases = result.databases;
            console.log(allDatabases);
            if (allDatabases.length > 0) {
                allDatabases.forEach((db) => {
                    if (db.name == `Keo_${UserDbName}`) {
                        console.log(`DB ${db.name} is already exits.`);
                        dbExists = true;
                        cb(true);
                    }
                })
                if (!dbExists) {
                    var newuri = config.cosmosDB.connectionString; // Используем основную строку подключения
                    console.log("Db Creating Process On...." + UserDbName)
                    var NewUserDb = mongoose.createConnection(newuri, {
                        dbName: `Keo_${UserDbName}`, // Добавляем имя базы данных
                        useNewUrlParser: true,
                        useUnifiedTopology: true
                    });
                    dataBaseSchema.createSchema(NewUserDb);
                    AdminDb.close();
                    cb(false);
                }
            }
        });
    });
}


module.exports = { createDB } 
