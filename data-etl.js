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
        const {loadJsonFile} = require('load-json-file');
        const path = require('path');
        const extractorsConfig = await loadJsonFile(path.join(__dirname, 'config', 'extractors.json'));
        const extractor = require(extractorsConfig[extractorName]);
        this.extractorInfo = {
            extractor,
            args
        };
    }

    setLoaderStream(loaderName, args) {
        const {loadJsonFile} = require('load-json-file');
        const path = require('path');
        const loadersConfig = await loadJsonFile(path.join(__dirname, 'config', 'loaders.json'));
        const loader = require(loadersConfig[loaderName]);
        this.loaderInfo = {
            loader,
            args
        };
    }

    setTransformerStreams(transformerInfos) {
        const {loadJsonFile} = require('load-json-file');
        const path = require('path');
        this.transformerInfos = transformerInfos.map(item => {
            const transformersConfig = await loadJsonFile(path.join(__dirname, 'config', 'transformers.json'));
            return {
                transformerName: require(transformersConfig[item.transformerName]),
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

        stream.pipeline(
            openExtractorStream(),
            ...openTransformerStreams(),
            openLoaderStream(),
            (err) => {}
        );
    }
}
