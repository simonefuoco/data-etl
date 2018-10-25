/**
 * data-etl module.
 * @author Simone Fuoco <simofstudies@gmail.com>
 * @module data-etl
 * @see module:transform
 */

class ETL
{
    constructor() {
        this.extractorInfo = {};
        this.transformerInfos = {};
        this.loaderInfo = {};
    }

    setExtractorStream(extractorName, args) {
        const {loadJsonFileModule} = require('load-json-file');
        const path = require('path');
        loadJsonFileModule.loadJsonFile(path.join(__dirname, 'config', 'extractors.json'))
        .then(extractorsConfig => {
            const extractor = require(extractorsConfig[extractorName]);
            this.extractorInfo = {
                extractor,
                args
            };
        });
    }

    async setLoaderStream(loaderName, args) {
        const {loadJsonFileModule} = require('load-json-file');
        const path = require('path');
        loadJsonFileModule.loadJsonFile(path.join(__dirname, 'config', 'loaders.json'))
        .then(loadersConfig => {
            const loader = require(loadersConfig[loaderName]);
            this.loaderInfo = {
                loader,
                args
            };
        });
    }

    async setTransformerStreams(transformerInfos) {
        const {loadJsonFileModule} = require('load-json-file');
        const path = require('path');
        this.transformerInfos = transformerInfos.map(item => {
            loadJsonFileModule.loadJsonFile(path.join(__dirname, 'config', 'transformers.json'))
            .then(transformersConfig => {
                return {
                    transformerName: require(transformersConfig[item.transformerName]),
                    args: item.args
                };
            });
        });
    }

    startStream() {
        const openExtractorStream = () => {
            this.extractorInfo.stream = this.extractorInfo.extractor.extract(this.extractorInfo.args);
            return this.extractorInfo.stream;
        };

        const openLoaderStream = () => {
            this.loaderInfo.stream = this.loaderInfo.loader.load(this.loaderInfo.args);
            return this.loaderInfo.stream;
        };

        const openTransformerStreams = () => {
            const streams = [];
            for (const transformerInfo of this.transformerInfos) {
                transformerInfo.stream = transformerInfo.transformer.transform(transformerInfo.args);
                streams.push(transformerInfo.stream);
            }
            return streams;
        };

        stream.pipeline(
            openExtractorStream(),
            ...openTransformerStreams(),
            openLoaderStream(),
            (err) => {}
        );
    }
}

module.exports = ETL;
