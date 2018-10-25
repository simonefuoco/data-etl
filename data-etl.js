/**
 * data-etl module.
 * @author Simone Fuoco <simofstudies@gmail.com>
 * @module data-etl
 * @see module:transform
 */


// const modules = {
//     "objmapper": require("data-etl-object-mapper"),
//     "generic": require("data-etl-generic-transformer"),
//     "psws": require("data-etl-prestashop-webservice"),
//     "mssql": require("data-etl-mssql")
// };

class ETL
{
    constructor() {
        this.extractorInfo = {};
        this.transformerInfos = {};
        this.loaderInfo = {};
    }

    setExtractorStream(extractorName, args) {
        const loadJsonFile = require('load-json-file');
        const path = require('path');
        const extractorsConfig = loadJsonFile.sync(path.join(__dirname, 'config', 'extractors.json'));
        const extractor = require(extractorsConfig[extractorName]);
        //const extractor = modules[extractorsConfig[extractorName]];
        this.extractorInfo = {
            extractor,
            args
        };
    }

    setLoaderStream(loaderName, args) {
        const loadJsonFile = require('load-json-file');
        const path = require('path');
        const loadersConfig = loadJsonFile.sync(path.join(__dirname, 'config', 'loaders.json'));
        const loader = require(loadersConfig[loaderName]);
        //const loader = modules[loadersConfig[loaderName]];
        this.loaderInfo = {
            loader,
            args
        };
    }

    setTransformerStreams(transformerInfos) {
        const loadJsonFile = require('load-json-file');
        const path = require('path');
        this.transformerInfos = transformerInfos.map(item => {
            const transformersConfig = loadJsonFile.sync(path.join(__dirname, 'config', 'transformers.json'));
            //require(path.join(__dirname, '..', transformersConfig[item.transformerName]))
            return {
                transformerName: require(transformersConfig[item.transformersName]),
                args: item.args
            };
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

        const stream = require('stream');

        stream.pipeline(
            openExtractorStream(),
            ...openTransformerStreams(),
            openLoaderStream(),
            (err) => {}
        );
    }
}

module.exports = ETL;
