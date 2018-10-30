const _ = require('lodash');
const loadJsonFile = require('load-json-file');
const path = require('path');

class Flow {

    constructor(args) {
        this._extractors = [];
        this._transformers = [];
        this._loader = {};
        this._aggregator = {};

        this.config = {
            extractors: this.getConfig('extractors'),
            transformers: this.getConfig('transformers'),
            loaders: this.getConfig('loaders'),
            aggregators: this.getConfig('aggregators')
        };
    }

    getConfig(name) {
        return loadJsonFile.sync(path.join(__dirname, 'config', `${name}.json`));
    }

    get extractors() {
        return this._extractors;
    }

    get transformers() {
        return this._transformers;
    }

    get loader() {
        return this._loader;
    }

    get aggregator() {
        return this._aggregator;
    }

    set extractors(extractors) {
        this.setExtractors(extractors);
    }

    set transformers(transformers) {
        this.setTransformers(transformers);
    }

    set loader(loaderInfo) {
        this.setLoader(loaderInfo.name, loaderInfo.args);
    }

    set aggregator(aggregatorInfo) {
        this.setAggregator(aggregatorInfo.name, aggregatorInfo.args);
    }

    setExtractors(extractors) {
        this.extractors = extractors.map(item => {
            let {Extractor} = require(this.config.extractors[item.name]);
            let extractor = new Extractor(item.args);
            return {
                extractor,
                name: item.name
            };
        }).bind(this);
    }

    setTransformers(transformers) {
        this.transformers = transformers.map(item => {
            let {Transformer} = require(this.config.transformers[item.name]);
            let transformer = new Transformer(item.args);
            return {
                transformer,
                name: item.name
            };
        }).bind(this);
    }

    setLoader(loaderName, args) {
        const {Loader} = require(this.config.loaders[loaderName]);
        this.loader = {
            loader: new Loader(args),
            name: loaderName
        };
    }

    setAggregator(aggregatorName, args) {
        const {Aggregator} = require(this.config.aggregators[aggregatorName]);
        let argsClone = _.cloneDeepWith(args);
        argsClone.extractors = this.extractors;
        this.aggregator = {
            aggregator: new Aggregator(argsClone),
            name: aggregatorName
        };
    }

    init() {
        let self = this;
        states = [];
        return new Promise((resolve, reject) => {
            self.extractors.map((item) => {
                item.once('data-etl-extractor-ready', () => {
                    states.push(true);
                    if (states.length === self.extractors.length) {
                        resolve();
                    }
                });
                item.init();
            });
        });
    }

    run() {
        let self = this;
        return new Promise((resolve, reject) => {
            while (!self.areExtractorsEmpty) {
                let obj = self.aggregator.aggregate()
                for (const transf of self.transformers) {
                    obj = transf.transformer.transform(obj);
                }
                self.loader.load(obj)
                .then(resolve)
                .catch((err) => {
                    reject(new Error("error run flow"));
                });
            }
        });
    }

    get areExtractorsEmpty() {
        let counters = [];
        for (const ext of this.extractors) {
            counters.push(ext.extractor.cache.counter);
        }
        let cond = counters.reduce((acc, value) => {
            return acc + value;
        }, 0);
        return cond === 0;
    }
}

module.exports.Flow = Flow;