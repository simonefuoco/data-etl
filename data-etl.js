/**
 * data-etl module.
 * @author Simone Fuoco <simofstudies@gmail.com>
 * @module data-etl
 * @see module:transform
 */

module.exports.ETL = function () {
    this.extractor = null;
    this.transformer = null;
    this.queue = [];
    this.loader = null;
};

module.exports.ETL.prototype.extract = function (extractor, args) {
    this.extractor = extractor(args);
};

module.exports.ETL.prototype.transform = function (transformer, args) {
    const transformer = transformer(args);
    this.transformer = this.transformer ? this.transformer.pipe(transformer) : transformer;
    this.queue.push(transformer);
};

module.exports.ETL.prototype.load = function (loader, args) {
    this.loader = loader(args);
};
