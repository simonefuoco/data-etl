const {MongoClient} = require('mongodb');

class Cache {

    constructor(args) {
        this.url = args.url;
        this.dbName = args.dbName;
        this.colName = args.colName;
    }

    count() {
        let self = this;
        return new Promise((resolve, reject) => {
            self.getCollection()
            .then((res) => {
                res.col.countDocuments({}, (err, result) => {
                    if(err) reject(new Error("cache count error"));
                    resolve(result);
                });
            })
            .catch((err) => {
                reject(new Error("cache count connection error"));
            });
        });
    }

    createOne(doc) {
        let self = this;
        return new Promise((resolve, reject) => {
            self.getCollection()
            .then((res) => {
                res.col.insertOne(doc, (err, result) => {
                    if(err || !result.insertedCount) {
                        reject(new Error("error cache create"));
                    }
                    res.client.close();
                    resolve();
                });
            })
            .catch((err) => {
                reject(new Error("error create one cache connection"));
            });
        });
    }

    getCollection() {
        let self = this;
        return new Promise((resolve, reject) => {
            MongoClient.connect(self.url, {useNewUrlParser: true})
            .then((client) => {
                const db = client.db(self.dbName);
                const col = db.collection(self.colName);
                let res = {
                    client,
                    db,
                    col
                };
                resolve(res);
            })
            .catch((err) => {
                reject(new Error("error cache connection"));
            });
        });
    }

    readAndDeleteOne(obj) {
        let self = this;
        let filter = obj ? obj : {};
        return new Promise((resolve, reject) => {
            self.getCollection()
            .then((res) => {
                res.col.findOneAndDelete(filter, (err, result) => {
                    if (err || !(result.ok === 1)) {
                        reject(new Error("error find one and delete"));
                    } else {
                        res.client.close();
                        delete result.value['_id'];
                        resolve(result.value);
                    }
                });
            })
            .catch((err) => {
                reject(new Error("error cache connection read and delete one"));
            });
        });
    }
}

module.exports.Cache = Cache;