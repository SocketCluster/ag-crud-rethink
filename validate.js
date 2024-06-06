function validateQuery(query, schema) {
  if (query == null) {
    throw new Error('Invalid query - The query was null or undefined');
  }
  let queryType = typeof query;
  if (queryType !== 'object') {
    throw new Error(`Invalid query - The query must be an object instead of ${queryType}`);
  }
  if (
    query.action !== 'create' &&
    query.action !== 'read' &&
    query.action !== 'update' &&
    query.action !== 'delete' &&
    query.action !== 'subscribe'
  ) {
    // Subscribe is not mentioned because it is added on the back end and is not a client-side concern.
    throw new Error('Invalid query - The query action must be either create, read, update or delete');
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
  if (query.sliceTo != null && typeof query.sliceTo !== 'number') {
    throw new Error('Invalid view query - The sliceTo property must be a number');
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
  let hasPrimaryKeys = viewSchema.primaryFields && viewSchema.primaryFields.length > 0;
  if (hasPrimaryKeys || (viewSchema.paramFields && viewSchema.paramFields.length > 0)) {
    validateRequiredViewParams(query.viewParams);
  }
  if (hasPrimaryKeys) {
    let missingFields = [];
    for (let field of viewSchema.primaryFields) {
      if (query.viewParams[field] == null) {
        missingFields.push(field);
      }
    }
    if (missingFields.length > 0) {
      throw new Error(
        `Invalid view query - The view ${
          query.view
        } under the type ${
          query.type
        } requires additional viewParams to meet primaryFields requirements. The following fields were missing or null: ${
          missingFields.join(', ')
        }`
      );
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

function getUnknownFieldErrorMessage(modelName) {
  return `The field was not part of the ${modelName} model schema`;
}

function throwFieldValidationError(modelName, field, errorMessage) {
  let crudValidationError = new Error(
    `Invalid ${
      field
    }: ${
      errorMessage
    }`
  );
  crudValidationError.name = 'CRUDValidationError';
  crudValidationError.model = modelName;
  crudValidationError.field = field;
  throw crudValidationError;
}

function validateValue(modelName, field, value, constraint) {
  try {
    return constraint.validate(value);
  } catch (error) {
    throwFieldValidationError(modelName, field, error.message);
  }
}

function throwModelValidationError(modelName, errorList) {
  let crudValidationError = new Error(
    `Invalid ${modelName} record`
  );
  crudValidationError.name = 'CRUDValidationError';
  crudValidationError.model = modelName;
  crudValidationError.fieldErrors = (errorList || []).map((error) => {
    return {
      field: error.field,
      message: error.message
    };
  });
  throw crudValidationError;
}

function enforceErrorCountLimit(modelName, errorList, options) {
  if (errorList.length >= options.maxErrorCount) {
    throwModelValidationError(modelName, errorList);
  }
}

function createModelValidator(modelName, modelSchemaFields, options) {
  return (record, allowPartial, throwImmediate) => {
    let errorList = [];
    let sanitizedRecord = {};
    if (allowPartial) {
      for (let [field, value] of Object.entries(record)) {
        try {
          let constraint = modelSchemaFields[field];
          if (constraint == null) {
            throwFieldValidationError(
              modelName,
              field,
              getUnknownFieldErrorMessage(modelName)
            );
          }
          sanitizedRecord[field] = validateValue(modelName, field, value, constraint);
        } catch (error) {
          if (throwImmediate) throw error;
          errorList.push(error);
        }
        enforceErrorCountLimit(modelName, errorList, options);
      }
      if (errorList.length) {
        throwModelValidationError(modelName, errorList);
      }
      return sanitizedRecord;
    }
    for (let field of Object.keys(record)) {
      try {
        if (modelSchemaFields[field] == null) {
          throwFieldValidationError(
            modelName,
            field,
            getUnknownFieldErrorMessage(modelName)
          );
        }
      } catch (error) {
        if (throwImmediate) throw error;
        errorList.push(error);
      }
      enforceErrorCountLimit(modelName, errorList, options);
    }
    for (let [field, constraint] of Object.entries(modelSchemaFields)) {
      try {
        let value = record[field];
        sanitizedRecord[field] = validateValue(modelName, field, value, constraint);
      } catch (error) {
        if (throwImmediate) throw error;
        errorList.push(error);
      }
      enforceErrorCountLimit(modelName, errorList, options);
    }
    if (errorList.length) {
      throwModelValidationError(modelName, errorList);
    }
    return sanitizedRecord;
  };
}

class TypeConstraint {
  constructor(validators, options) {
    this.validators = validators || {};
    this.options = options || {};
  }

  required() {
    return this.createSubConstraintWithValidators(
      null,
      { required: true }
    );
  }

  allowNull() {
    return this.createSubConstraintWithValidators(
      null,
      { allowNull: true }
    );
  }

  validator(fn) {
    return this.createSubConstraintWithValidators({
      validator: fn
    });
  }

  validate(value) {
    if (
      (this.options.allowNull && value === null) ||
      (!this.options.required && value === undefined)
    ) {
      return value;
    }
    for (let validator of Object.values(this.validators)) {
      value = validator(value);
    }
    return value;
  }

  toString() {
    let validatorInfo = {...this.validators};
    if (this.options.allowNull) {
      validatorInfo.allowNull = {};
    }
    if (this.options.required) {
      validatorInfo.required = {};
    }
    return `[constraint ${
      Object.entries(validatorInfo).map(
        ([key, validatorFn]) => {
          return `${
            key
          }${
            Array.isArray(validatorFn.args) && validatorFn.args.length ? `(${validatorFn.args})` : ''
          }`;
        }
      ).join(', ')
    }]`;
  }
}

const ALPHANUM_REGEX = /^[0-9a-zA-Z]*$/;
const LOWERCASE_REGEX = /^[^A-Z]*$/;
const UPPERCASE_REGEX = /^[^a-z]*$/;
const EMAIL_REGEX = /^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$/i;

const UUID_REGEXES = {
  3: /^[0-9A-F]{8}-[0-9A-F]{4}-3[0-9A-F]{3}-[0-9A-F]{4}-[0-9A-F]{12}$/i,
  4: /^[0-9A-F]{8}-[0-9A-F]{4}-4[0-9A-F]{3}-[89AB][0-9A-F]{3}-[0-9A-F]{12}$/i,
  5: /^[0-9A-F]{8}-[0-9A-F]{4}-5[0-9A-F]{3}-[89AB][0-9A-F]{3}-[0-9A-F]{12}$/i,
  all: /^[0-9A-F]{8}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{12}$/i
};

let stringValidators = {
  string: (arg) => {
    return (value) => {
      if (typeof value !== 'string') {
        throw new Error('Value must be a string');
      }
      return value;
    };
  },
  min: (arg) => {
    return (value) => {
      if (value.length < arg) {
        throw new Error(`Value must be at least ${arg} character${arg === 1 ? '' : 's'} in length`);
      }
      return value;
    };
  },
  max: (arg) => {
    return (value) => {
      if (value.length > arg) {
        throw new Error(`Value cannot exceed ${arg} character${arg === 1 ? '' : 's'} in length`);
      }
      return value;
    };
  },
  length: (arg) => {
    return (value) => {
      if (value.length !== arg) {
        throw new Error(`Value must be ${arg} character${arg === 1 ? '' : 's'} long`);
      }
      return value;
    };
  },
  alphanum: (arg) => {
    return (value) => {
      if (!value.match(ALPHANUM_REGEX)) {
        throw new Error('Value must be alphanumeric');
      }
      return value;
    };
  },
  regex: (argA, argB) => {
    let regex = new RegExp(argA, argB);
    return (value) => {
      if (!value.match(regex)) {
        throw new Error('Value must adhere to the required regular expression format');
      }
      return value;
    };
  },
  email: (arg) => {
    return (value) => {
      if (!value.match(EMAIL_REGEX)) {
        throw new Error('Value must be an email address');
      }
      return value;
    };
  },
  lowercase: (arg) => {
    return (value) => {
      if (!value.match(LOWERCASE_REGEX)) {
        throw new Error('Value must be in lowercase');
      }
      return value;
    };
  },
  uppercase: (arg) => {
    return (value) => {
      if (!value.match(UPPERCASE_REGEX)) {
        throw new Error('Value must be in uppercase');
      }
      return value;
    };
  },
  enum: (arg) => {
    return (value) => {
      if (!arg.includes(value)) {
        throw new Error(`Value must be one of the following: ${arg.join(', ')}`);
      }
      return value;
    };
  },
  uuid: (arg) => {
    return (value) => {
      let regex = UUID_REGEXES[arg || 'all'];
      if (!value.match(regex)) {
        throw new Error(
          `Value must be a UUID${
            arg ? ` (v${arg})` : ''
          }`
        );
      }
      return value;
    };
  },
  multi: () => {
    return (value) => {
      return value;
    };
  },
  blob: () => {
    return (value) => {
      return value;
    };
  }
};

class StringTypeConstraint extends TypeConstraint {
  createSubConstraintWithValidators(newValidators, options) {
    return new StringTypeConstraint(
      {
        ...this.validators,
        ...newValidators
      },
      {
        ...this.options,
        ...options
      }
    );
  }

  createSubConstraint(constraintName, args, options) {
    let newValidators = {};
    if (constraintName != null) {
      let validatorFn = stringValidators[constraintName](...args);
      validatorFn.args = args;
      newValidators[constraintName] = validatorFn;
    }
    return this.createSubConstraintWithValidators(newValidators, options);
  }

  min(...args) {
    return this.createSubConstraint('min', args);
  }

  max(...args) {
    return this.createSubConstraint('max', args);
  }

  length(...args) {
    return this.createSubConstraint('length', args);
  }

  alphanum() {
    return this.createSubConstraint('alphanum', []);
  }

  regex(...args) {
    return this.createSubConstraint('regex', args);
  }

  email() {
    return this.createSubConstraint('email', []);
  }

  lowercase() {
    return this.createSubConstraint('lowercase', []);
  }

  uppercase() {
    return this.createSubConstraint('uppercase', []);
  }

  enum(...args) {
    return this.createSubConstraint('enum', args);
  }

  uuid(...args) {
    return this.createSubConstraint('uuid', args);
  }

  multi() {
    return this.createSubConstraint('multi', []);
  }

  blob() {
    return this.createSubConstraint('blob', []);
  }
}

let numberValidators = {
  number: (arg) => {
    return (value) => {
      if (typeof value !== 'number') {
        throw new Error('Value must be a number');
      }
      return value;
    };
  },
  min: (arg) => {
    return (value) => {
      if (value < arg) {
        throw new Error(`Value must be at least ${arg}`);
      }
      return value;
    };
  },
  max: (arg) => {
    return (value) => {
      if (value > arg) {
        throw new Error(`Value cannot exceed ${arg}`);
      }
      return value;
    };
  },
  integer: (arg) => {
    return (value) => {
      if (!Number.isInteger(value)) {
        throw new Error('Value must be an integer');
      }
      return value;
    };
  }
};

class NumberTypeConstraint extends TypeConstraint {
  createSubConstraintWithValidators(newValidators, options) {
    return new NumberTypeConstraint(
      {
        ...this.validators,
        ...newValidators
      },
      {
        ...this.options,
        ...options
      }
    );
  }

  createSubConstraint(constraintName, args, options) {
    let newValidators = {};
    if (constraintName != null) {
      let validatorFn = numberValidators[constraintName](...args);
      validatorFn.args = args;
      newValidators[constraintName] = validatorFn;
    }
    return this.createSubConstraintWithValidators(newValidators, options);
  }

  min(...args) {
    return this.createSubConstraint('min', args);
  }

  max(...args) {
    return this.createSubConstraint('max', args);
  }

  integer() {
    return this.createSubConstraint('integer', []);
  }
}

let booleanValidators = {
  boolean: (arg) => {
    return (value) => {
      if (typeof value !== 'boolean') {
        throw new Error('Value must be a boolean');
      }
      return value;
    };
  }
};

class BooleanTypeConstraint extends TypeConstraint {
  createSubConstraintWithValidators(newValidators, options) {
    return new BooleanTypeConstraint(
      {
        ...this.validators,
        ...newValidators
      },
      {
        ...this.options,
        ...options
      }
    );
  }
}

class AnyTypeConstraint extends TypeConstraint {
  createSubConstraintWithValidators(newValidators, options) {
    return new AnyTypeConstraint(
      {
        ...this.validators,
        ...newValidators
      },
      {
        ...this.options,
        ...options
      }
    );
  }
}

let typeBuilder = {
  string: () => {
    return new StringTypeConstraint({
      string: stringValidators.string()
    });
  },
  number: () => {
    return new NumberTypeConstraint({
      number: numberValidators.number()
    });
  },
  boolean: () => {
    return new BooleanTypeConstraint({
      boolean: booleanValidators.boolean()
    });
  },
  any: () => {
    return new AnyTypeConstraint();
  }
};

module.exports = {
  validateQuery,
  createModelValidator,
  typeBuilder
};
