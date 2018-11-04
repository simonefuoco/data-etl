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
        this.setLoader(loaderInfo.name, loaderInfo.args, loaderInfo.loader);
    }

    set aggregator(aggregatorInfo) {
        this.setAggregator(aggregatorInfo.name, aggregatorInfo.args, aggregatorInfo.aggregator);
    }

    setExtractors(extractors) {
        let self = this;
        this._extractors = extractors.map(item => {

            let Extractor;

            if (item.extractor) {
                Extractor = item.extractor;
            } else {
                Extractor = require(self.config.extractors[item.name]).Extractor;
            }

            let extractor = new Extractor(item.args);
            return {
                extractor,
                name: item.name
            };
        });
    }

    setTransformers(transformers) {
        let self = this;
        this._transformers = transformers.map(item => {

            let Transformer;

            if (item.transformer) {
                Transformer = item.transformer;
            } else {
                Transformer = require(self.config.transformers[item.name]).Transformer;
            }

            let transformer = new Transformer(item.args);
            return {
                transformer,
                name: item.name
            };
        });
    }

    setLoader(loaderName, args, loader) {

        let Loader;

        if (loader) {
            Loader = loader;
        } else {
            Loader = require(this.config.loaders[loaderName]).Loader;
        }

        this._loader = {
            loader: new Loader(args),
            name: loaderName
        };
    }

    setAggregator(aggregatorName, args, aggregator) {

        let Aggregator;

        if (aggregator) {
            Aggregator = aggregator;
        } else {
            Aggregator = require(this.config.aggregators[aggregatorName]).Aggregator;
        }

        let argsClone = _.cloneDeepWith(args);
        argsClone.extractors = this.extractors;
        this._aggregator = {
            aggregator: new Aggregator(argsClone),
            name: aggregatorName
        };
    }

    init() {
        let self = this;
        let states = [];
        return new Promise((resolve, reject) => {
            self.extractors.map((item) => {
                item.extractor.once('data-etl-extractor-ready', () => {
                    states.push(true);
                    if (states.length === self.extractors.length) {
                        resolve();
                    }
                });
                item.extractor.init();
            });
        });
    }

    async run() {
        let self = this;

        let cond = await this.areExtractorsEmpty();
        while (!cond) {
            let obj = await self.aggregator.aggregator.aggregate();
            for (const transf of self.transformers) {
                obj = await transf.transformer.transform(obj);
            }
            let isLoaded = await self.loader.loader.load(obj);
            if (!isLoaded) throw new Error("run something not loaded");

            cond = await this.areExtractorsEmpty();
        }
    }

    areExtractorsEmpty() {
        let self = this;
        let counters = [];
        let promises = [];

        return new Promise((resolve, reject) => {
            for (const ext of self.extractors) {
                promises.push(ext.extractor.cache.count());
            }
            Promise.all(promises)
            .then((values) => {
                for (const value of values) {
                    counters.push(value);
                }
                let cond = counters.reduce((acc, value) => acc + value, 0);
                resolve(cond === 0);
            })
            .catch((err) => {
                reject(new Error("error areExtractorsEmpty in flow"));
            });
        });
    }
}

module.exports.Flow = Flow;