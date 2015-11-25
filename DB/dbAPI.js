var MongoClient = require('mongodb').MongoClient;

var driverMaping = {
        "2.4": "1.4.34",
        "2.6": "1.4.34",
        "2.8": "1.4.34",
        "3.0": "1.4.34"

};

function BookmarkAPI(){

    function addCommonData(Obj) {
        Obj.limit = Obj.limit ? Obj.limit : 10;
        Obj.skip = Obj.skip ? Obj.skip : 0;
        Obj.sort = Obj.sort ? Obj.sort : {"_id": 1};
        Obj.QuerySelect = Obj.QuerySelect ? Obj.QuerySelect : {};
        Obj.Query_obj = Obj.Query_obj ? Obj.Query_obj : {};
        Obj.driver = Obj.driver ? Obj.driver : "native";
        Obj.connString = Obj.connString ? Obj.connString : "";
        Obj.DBVersion = Obj.DBVersion ? Obj.DBVersion : '2.6.1';
        Obj.data_to_update = Obj.data_to_update ? Obj.data_to_update : {};

        if(Obj.singleData){
            Obj.limit = 1;
        }

                Obj.connString = "mongodb://10.0.0.51:10040/"+ Obj.DBName

    }

    function extractDBVersion(DBVersion) {
        var tokens = DBVersion.split('.');
        tokens.splice(2, tokens.length);
        return tokens.join('.');
    }

    function getMongoClient(Obj) {
        var mongoClient;
        if (Obj.driver == "native") {
            mongoClient = require('mongodb').MongoClient;
        }
        else {
            var driverVersion = driverMaping[extractDBVersion(Obj.DBVersion)];
            try {
                mongoClient = require('mongodb' + driverVersion).MongoClient;
            }
            catch (ex) {
                //If loading the driver fails we try to execute query with native driver
                console.log("Failed to load driver. Loading native driver instead.");
                mongoClient = require('mongodb').MongoClient;
            }
        }
        return mongoClient;
    }

    this.getConnection = function (Obj, callBack) {
        var DBObj = Obj;
        addCommonData(DBObj);

        var mongoClient = getMongoClient(DBObj);

        mongoClient.connect(DBObj.connString, {server: {poolSize: 1}}, function (err, dbObj) {
            if (err) {
                console.log("error", err);
                callBack("error", null)
            } else {
                console.log("Connection Success");
                callBack(null, dbObj);
            }
        });
    };

    /* sample object***********************************
     DBName: 'test',
     collectionName: 'test',
     limit: 10,
     skip: 0,
     sort: {_id: 1},
     QuerySelect: { _id: 0 },
     Query_obj: {},
     driver: 'external',
     connString: 'mongodb://localhost:27017/test'
     DBVersion: '2.6.1'
     * @param callBack
     */

    this.readData = function (Obj, callBack) {
        var DBObj = Obj;

        this.getConnection(DBObj, function (err, conn) {
            if (err) {
                console.log("error", err);
                callBack(err, null);
            }
            else {
                conn.collection(DBObj.collectionName)
                    .find(DBObj.Query_obj, DBObj.QuerySelect)
                    .limit(DBObj.limit)
                    .skip(DBObj.skip)
                    .sort(DBObj.sort)
                    .toArray(function (err, result) {
                        disconnect(conn);
                        if (err) {
                            console.log("error "+ JSON.stringify(err));
                            callBack(err, null);
                        }
                        else {
                            console.log("readDataSuccess");
                            callBack(null, result);
                        }
                    });
            }
        });
    }

    this.insertData = function (Obj, callBack) {
        var DBObj = Obj;

        this.getConnection(DBObj, function (err, conn) {
            if (err) {
                console.log("error", err);
                callBack(err, null);
            }
            else {
                console.log("AcquiredConnection");
                conn.collection(DBObj.collectionName)
                    .insert(DBObj.data, function (err, result) {
                        disconnect(conn);
                        if (err) {
                            console.log("error", "insertData. Cause: " + JSON.stringify(err));
                            callBack(err, null);
                        }
                        else {
                            console.log("silly", "insertDataSuccess");
                            callBack(null, result);
                        }
                    });
            }
        });
    }

    this.updateData = function (Obj, callBack) {
        var DBObj = Obj;

        this.getConnection(DBObj, function (err, conn) {
            if (err) {
                console.log("ErrorGettingConnection");
                callBack(err, null);
            }
            else {
                console.log("AcquiredConnection");
                conn.collection(DBObj.collectionName)
                    .update(DBObj.Query_obj, DBObj.data_to_update, DBObj.options, function (err, result, doc) {
                        disconnect(conn);
                        if (err) {
                            console.log("updateDataError. Cause: " + JSON.stringify(err));
                            callBack(err, null);
                        }
                        else {
                            console.log("updateDataSuccess");
                            callBack(null, doc);
                        }
                    });
            }
        });
    }

    this.removeData = function (Obj, callBack) {
        var DBObj = Obj;

        this.getConnection(DBObj, function (err, conn) {
            if (err) {
                console.log("ErrorGettingConnection");
                callBack(err, null);
            }
            else {
                console.log("AcquiredConnection");
                conn.collection(DBObj.collectionName)
                    .remove(DBObj.Query_obj, function (err, result, raw) {
                        disconnect(conn);
                        if (err) {
                            console.log("removeData. Cause: " + JSON.stringify(err));
                            callBack(err, null);
                        }
                        else {
                            console.log("removeDataSuccess");
                            callBack(null, raw);
                        }
                    });
            }
        });
    }




    function disconnect(connection) {
        connection.close();
    }



}
module.exports = BookmarkAPI;