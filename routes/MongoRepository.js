const { Binary, MongoClient } = require('mongodb');
let BSON = require('bson');
let ObjectId = BSON.ObjectId;
let uuid = require('mongo-uuid');
let _ = require('lodash');

let MongoRepository = function (connection, dbname, collection) {
    this.isActivate = false;
    this.connectionString = connection;
    this.databaseName = dbname;
    this.collectionName = collection;
};

MongoRepository.prototype.activate = function(callback = null, forceActivate = false) {
    if (this.isActivate && !forceActivate) return NONE;
    MongoClient.connect(this.connectionString, function (err, db) {
        if (err) {
            throw err;
        }
        if (callback !== null){
            callback(db, err);
        }
        console.log('mongodb connected.');
        db.close();
    });
};

MongoRepository.prototype.find = function(callback = null, guidColumnNames = null, filter = null) {
    let dbName = this.databaseName;
    let cName = this.collectionName;
    MongoClient.connect(this.connectionString, function (err, database) {
        if (err) {
            throw err;
        }
        let db = database.db(dbName);
        if (filter === null){
            db.collection(cName).find(filter).toArray(function (err, entities) {
                ConversionUuidToStringForEntities(guidColumnNames, entities);
                callback(db, err, entities);
            });
        }
        else {
            db.collection(cName).find({}).toArray(function (err, entities) {
                ConversionUuidToStringForEntities(guidColumnNames, entities);
                callback(db, err, entities);
            });
        }
    });
};
function ConversionStringToObjectIdForEntities(entities) {
    if (entities !== null) {
        _.forEach(entities, function (entity) {
            entity._id = new ObjectId(entity._id);
        });
    }
}
function ConversionUuidToStringForEntities(guidColumnNames, entities) {
    if (guidColumnNames !== null) {
        _.forEach(guidColumnNames, function (columnName) {
            _.forEach(entities, function (entity) {
                let oldId = entity[columnName];
                entity[columnName] = uuid.stringify(oldId);
            });
        });
    }
}
function ConversionUuidToStringForEntity(guidColumnNames, entity) {
    if (guidColumnNames !== null) {
        _.forEach(guidColumnNames, function (columnName) {
            let oldId = entity[columnName];
            entity[columnName] = uuid.stringify(oldId);
        });
    }
}
function ConversionStringToUuidForEntities(guidColumnNames, entities) {
    if (guidColumnNames !== null) {
        _.forEach(guidColumnNames, function (columnName) {
            _.forEach(entities, function (entity) {
                let oldId = entity[columnName];
                entity[columnName] = uuid.parse(Binary, oldId);
            });
        });
    }
}
function ConversionStringToUuidForEntity(guidColumnNames, entity) {
    if (guidColumnNames !== null) {
        _.forEach(guidColumnNames, function (columnName) {
            let oldId = entity[columnName];
            entity[columnName] = uuid.parse(Binary, oldId);
        });
    }
}
MongoRepository.prototype.save = function(entity, guidColumnNames = null, callback = null) {
    let dbName = this.databaseName;
    let cName = this.collectionName;
    MongoClient.connect(this.connectionString, function (err, database) {
        if (err) {
            throw err;
        }
        //objectId、uuidを変換
        entity._id = new ObjectId(entity._id);
        ConversionStringToUuidForEntity(guidColumnNames, entity);

        let db = database.db(dbName);
        db.collection(cName).save(entity, function (err, res) {
            callback(db, err, res);
        });
    });
};
MongoRepository.prototype.saveMulti = function(entities, guidColumnNames = null, callback = null) {
    let dbName = this.databaseName;
    let cName = this.collectionName;
    MongoClient.connect(this.connectionString, function (err, database) {
        if (err) {
            throw err;
        }
        //objectId、uuidを変換
        ConversionStringToObjectIdForEntities(entities);
        ConversionStringToUuidForEntities(guidColumnNames, entities);

        let db = database.db(dbName);
        let colle = db.collection(cName);
        colle.update(entities, {upsert: true, multi: true}, function (err, res) {
            callback(db, err, res);
        });
    });
};
module.exports = MongoRepository;