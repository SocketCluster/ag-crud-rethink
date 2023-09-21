function validateQuery(query, schema) {
  if (query == null) {
    throw new Error('Invalid query - The query was null or undefined');
  }
  let queryType = typeof query;
  if (queryType !== 'object') {
    throw new Error(`Invalid query - The query must be an object instead of ${queryType}`);
  }
  if (typeof query.type !== 'string') {
    throw new Error('Invalid query - The query type must be a string');
  }
  if (!schema[query.type]) {
    throw new Error(`Invalid query - The query type ${query.type} was not defined on the schema`);
  }

  let fieldIsSet = query.field != null;
  let idIsSet = query.id != null;
  if (fieldIsSet) {
    let fieldType = typeof query.field;
    if (fieldType !== 'string') {
      throw new Error(`Invalid field query - The field property must be a string instead of ${fieldType}`);
    }
    if (!idIsSet) {
      throw new Error('Invalid field query - The query must have an id property');
    }
  }
  if (idIsSet) {
    let idType = typeof query.id;
    if (idType !== 'string') {
      throw new Error(`Invalid resource query - The resource id must be a string instead of ${idType}`);
    }
  }
  let viewIsSet = query.view != null;
  if (viewIsSet) {
    validateViewQuery(query, schema);
  }
  if (query.offset != null && typeof query.offset !== 'number') {
    throw new Error('Invalid view query - The offset property must be a number');
  }
  if (query.pageSize != null && typeof query.pageSize !== 'number') {
    throw new Error('Invalid view query - The pageSize property must be a number');
  }
  if (query.getCount != null && typeof query.getCount !== 'boolean') {
    throw new Error('Invalid view query - The getCount property must be a boolean');
  }
}

function validateViewQuery(query, schema) {
  let viewType = typeof query.view;
  if (viewType !== 'string') {
    throw new Error(`Invalid view query - The view property must be a string instead of ${viewType}`);
  }
  if (!schema[query.type].views || !schema[query.type].views[query.view]) {
    throw new Error(`Invalid view query - The view ${query.view} was not defined in the schema under the type ${query.type}`);
  }
  let viewSchema = schema[query.type].views[query.view];
  let hasPrimaryKeys = viewSchema.primaryKeys && viewSchema.primaryKeys.length > 0;
  if (hasPrimaryKeys || (viewSchema.paramFields && viewSchema.paramFields.length > 0)) {
    validateRequiredViewParams(query.viewParams);
  }
  if (hasPrimaryKeys) {
    let missingFields = [];
    viewSchema.primaryKeys.forEach((field) => {
      if (query.viewParams[field] === undefined) {
        missingFields.push(field);
      }
    });
    if (missingFields.length > 0) {
      throw new Error(`Invalid view query - The view ${query.view} under the type ${query.type} requires additional fields to meet primaryKeys requirements. Missing: ${missingFields.join(', ')}`);
    }
  }
}

function validateRequiredViewParams(viewParams) {
  if (viewParams == null) {
    throw new Error(`Invalid view query - The view ${query.view} under the type ${query.type} expects viewParams but it was null or undefined`);
  }
  let viewParamsType = typeof viewParams;
  if (viewParamsType !== 'object') {
    throw new Error(`Invalid view query - The view ${query.view} under the type ${query.type} expects viewParams to be an object instead of ${viewParamsType}`);
  }
}

module.exports = {
  validateQuery
};
