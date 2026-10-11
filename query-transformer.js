const { getOwnProperty } = require('./validate');

let getViewMetaData = function (options, type, viewName) {
  let typeSchema = getOwnProperty(options.schema, type) || {};
  let viewSchema = getOwnProperty(typeSchema.views, viewName) || {};

  return Object.assign({}, viewSchema);
};

module.exports.constructTransformedRethinkQuery = function (options, rethinkQuery, type, viewName, viewParams) {
  let viewMetaData = getViewMetaData(options, type, viewName);

  let sanitizedViewParams = {};
  if (typeof viewParams === 'object' && viewParams != null) {
    for (let field of (viewMetaData.paramFields || [])) {
      let value = viewParams[field];
      sanitizedViewParams[field] = value === undefined ? null : value;
    }
  }

  let transformFn = viewMetaData.transform;
  if (transformFn) {
    rethinkQuery = transformFn(rethinkQuery, options.rethink, sanitizedViewParams);
  }

  return rethinkQuery;
};
