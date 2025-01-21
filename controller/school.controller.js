//school.controller.js
'use strict';
var jwt = require('../commonservice/jwtverify');
/**
 * Create School in Particular User Database 
 */
module.exports.CreateSchool = function (req, res, next) {
    var School = DBConnectionsList[jwt.jwtverify(req)].studentModel;
    School.create(req.body, function (err, success) {
        if (err) {
            return res.status(402).send(err);
        }
        if (success) {
            return res.status(200).send(success);
        } else {
            return res.status(404).send('School not created.');
        }
    });
};

module.exports.getAllSchool = function (req, res, next) {
    var School = DBConnectionsList[jwt.jwtverify(req)].studentModel;
    School.find({}, function (err, user) {
        if (err) {
            return res.status(402).send(err);
        }
        if (user) {
            return res.status(200).send(user);
        }
    });
}

module.exports.searchSchoolsByName = function (req, res, next) {
    const School = DBConnectionsList[jwt.jwtverify(req)].studentModel;
    const keyword = req.query.keyword;

    if (!keyword) {
        return res.status(400).send("Keyword is required for search.");
    }

    School.find({ "SchoolName": { "$regex": keyword, "$options": "i" } }, function (err, schools) {
        if (err) {
            return res.status(402).send(err);
        }
        if (schools) {
            return res.status(200).send(schools);
        } else {
            return res.status(404).send("No schools found.");
        }
    });
};

module.exports.getSchoolById = function (req, res, next) {
    const School = DBConnectionsList[jwt.jwtverify(req)].studentModel;
    const schoolId = req.params.schoolId;

    if (!schoolId) {
        return res.status(400).send("SchoolId is required.");
    }

    School.findOne({ "SchoolId": schoolId }, function (err, school) {
        if (err) {
            return res.status(402).send(err);
        }
        if (school) {
            return res.status(200).send(school);
        } else {
            return res.status(404).send("School not found.");
        }
    });
};

module.exports.getRedisServerData = function (req, res, next) {
    let token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VyTmFtZSI6IkFydWwiLCJDTmFtZSI6IkRCMSIsIlBlcm1pc3Npb24iOlsidXBkYXRlIiwiZ2V0IGFsbCBzY2hvb2wgcmVjb3JkIiwiZGVsZXRlIl0sImlhdCI6MTUzNzI1OTQ4MywiaXNzIjoiS2VvIHBsdXMgTE1TIn0.N75JL4YEKEMZrZvSGdnSiAQpm_2G6VPRDyUVlAKTTog";
    redisClient.get(token, function (err, reply) {
        let data = JSON.parse(reply);
        console.log(typeof data);
        res.send(data.Permission);
    });
}