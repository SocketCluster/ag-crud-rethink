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
      if (query.viewParams[field] == null) {
        missingFields.push(field);
      }
    });
    if (missingFields.length > 0) {
      throw new Error(
        `Invalid view query - The view ${
          query.view
        } under the type ${
          query.type
        } requires additional viewParams to meet primaryKeys requirements. The following fields were missing or null: ${
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

function validateRecordAgainstConstraint(modelName, field, value, constraint) {
  try {
    if (constraint == null) {
      throw new Error(getUnknownFieldErrorMessage(modelName));
    }
    if (
      (!constraint.options.required && value === undefined) ||
      (constraint.options.allowNull && value === null)
    ) {
      return value;
    }
    return constraint.validate(value);
  } catch (error) {
    let crudValidationError = new Error(
      `Invalid ${
        field
      }: ${
        error.message
      }`
    );
    crudValidationError.name = 'CRUDValidationError';
    crudValidationError.model = modelName;
    crudValidationError.field = field;
    throw crudValidationError;
  }
}

function createVerifier(modelName, modelSchemaFields) {
  return (record, allowPartial) => {
    let sanitizedRecord = {};
    if (allowPartial) {
      for (let [field, value] of Object.entries(record)) {
        sanitizedRecord[field] = validateRecordAgainstConstraint(
          modelName,
          field,
          value,
          modelSchemaFields[field]
        );
      }
      return sanitizedRecord;
    }
    for (let [field, constraint] of Object.entries(modelSchemaFields)) {
      let value = record[field];
      sanitizedRecord[field] = validateRecordAgainstConstraint(
        modelName,
        field,
        value,
        constraint
      );
    }
    for (let field of Object.keys(record)) {
      if (modelSchemaFields[field] == null) {
        let crudValidationError = new Error(
          `Invalid ${
            field
          }: ${
            getUnknownFieldErrorMessage(modelName)
          }`
        );
        crudValidationError.name = 'CRUDValidationError';
        crudValidationError.model = modelName;
        crudValidationError.field = field;
        throw crudValidationError;
      }
    }
    return sanitizedRecord;
  };
}

let genericValidators = {
  default: (arg) => {
    return (value) => {
      if (value == null) {
        return arg;
      }
      return value;
    };
  }
};

class TypeConstraint {
  constructor(validators, options) {
    this.validators = validators || {};
    this.options = options || {};
  }

  validate(value) {
    let sanitizedValue = value;
    for (let validator of Object.values(this.validators)) {
      sanitizedValue = validator(sanitizedValue);
    }
    return sanitizedValue;
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
  ...genericValidators,
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
        throw new Error(`Value must be at least ${arg} characters in length`);
      }
      return value;
    };
  },
  max: (arg) => {
    return (value) => {
      if (value.length > arg) {
        throw new Error(`Value cannot exceed ${arg} characters in length`);
      }
      return value;
    };
  },
  length: (arg) => {
    return (value) => {
      if (value.length !== arg) {
        throw new Error(`Value must be ${arg} characters long`);
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
  regex: (arg) => {
    let regex = new RegExp(arg[0], arg[1]);
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
  }
};

class StringTypeConstraint extends TypeConstraint {
  createSubConstraint(constraintName, arg, options) {
    let newValidators = {};
    if (constraintName != null) {
      newValidators[constraintName] = stringValidators[constraintName](arg);
    }
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

  required() {
    return this.createSubConstraint(
      null,
      {
        ...this.options,
        required: true
      },
      { required: true }
    );
  }

  allowNull() {
    return this.createSubConstraint(
      null,
      {
        ...this.options,
        allowNull: true
      },
      { allowNull: true }
    );
  }

  default(arg) {
    return this.createSubConstraint('default', arg);
  }

  min(arg) {
    return this.createSubConstraint('min', arg);
  }

  max(arg) {
    return this.createSubConstraint('max', arg);
  }

  length(arg) {
    return this.createSubConstraint('length', arg);
  }

  alphanum() {
    return this.createSubConstraint('alphanum');
  }

  regex(arg) {
    return this.createSubConstraint('regex', arg);
  }

  email() {
    return this.createSubConstraint('email');
  }

  lowercase() {
    return this.createSubConstraint('lowercase');
  }

  uppercase() {
    return this.createSubConstraint('uppercase');
  }

  enum(arg) {
    return this.createSubConstraint('enum', arg);
  }

  uuid(arg) {
    return this.createSubConstraint('uuid', arg);
  }
}

let numberValidators = {
  ...genericValidators,
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
        throw new Error(`Value must be less than ${arg}`);
      }
      return value;
    };
  },
  max: (arg) => {
    return (value) => {
      if (value > arg) {
        throw new Error(`Value cannot be greater than ${arg}`);
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
  createSubConstraint(constraintName, arg, options) {
    let newValidators = {};
    if (constraintName != null) {
      newValidators[constraintName] = numberValidators[constraintName](arg);
    }
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

  required() {
    return this.createSubConstraint(
      null,
      {
        ...this.options,
        required: true
      },
      { required: true }
    );
  }

  allowNull() {
    return this.createSubConstraint(
      null,
      {
        ...this.options,
        allowNull: true
      },
      { allowNull: true }
    );
  }

  default(arg) {
    return this.createSubConstraint('default', arg);
  }

  min(arg) {
    return this.createSubConstraint('min', arg);
  }

  max(arg) {
    return this.createSubConstraint('max', arg);
  }

  integer() {
    return this.createSubConstraint('integer');
  }
}

let booleanValidators = {
  ...genericValidators,
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
  createSubConstraint(constraintName, arg, options) {
    let newValidators = {};
    if (constraintName != null) {
      newValidators[constraintName] = booleanValidators[constraintName](arg);
    }
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

  required() {
    return this.createSubConstraint(
      null,
      {
        ...this.options,
        required: true
      },
      { required: true }
    );
  }

  allowNull() {
    return this.createSubConstraint(
      null,
      {
        ...this.options,
        allowNull: true
      },
      { allowNull: true }
    );
  }

  default(arg) {
    return this.createSubConstraint('default', arg);
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
  }
};

module.exports = {
  validateQuery,
  createVerifier,
  typeBuilder
};
