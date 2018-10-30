const {MongoClient} = require('mongodb');

class Cache {

    constructor(args) {
        this._counter = 0;
        this.url = args.url;
        this.dbName = args.dbName;
        this.colName = args.colName;
    }

    get counter() {
        return this._counter;
    }

    get isEmpty() {
        return this.counter === 0;
    }

    createOne(doc) {
        let self = this;
        this._counter++;
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
        this._counter--;
        return new Promise((resolve, reject) => {
            self.getCollection()
            .then((res) => {
                res.col.findOneAndDelete(filter, (err, result) => {
                    if (err || !(result.ok === 1)) {
                        reject(new Error("error find one and delete"));
                    } else {
                        res.client.close();
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