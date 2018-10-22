module.exports.extract = async (importers, args) => {
    let promises = [];
    args.importers.forEach(importer => {
        promises.push(importer.import(args));
    });
    await Promise.all(promises);
};

module.exports.transform = async (transformer, args)  => {
    await transformer(args);
};

module.exports.load = function (loader, args) {
    await loader(args);
};

module.exports = ETL;
