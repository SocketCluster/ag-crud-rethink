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

function createVerifier(modelName, modelSchemaFields) {
  return (record) => {
    let sanitizedRecord = {};
    for (let [key, constraint] of Object.entries(modelSchemaFields)) {
      sanitizedRecord[key] = constraint.validate(record[key]);
    }
    return sanitizedRecord;
  };
}

let genericValidators = {
  required: (arg) => {
    return (value) => {
      if (value === undefined) {
        throw new Error('Value cannot be undefined');
      };
      return value;
    };
  },
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
  constructor(validators) {
    this.validators = validators || {};
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
  constructor(constraints, validators) {

  }

  required() {
    return new StringTypeConstraint([...this.constraints, {name: 'required'}]);
  }

  allowNull(arg) {
    return new StringTypeConstraint(
      [...this.constraints],
      {
        string: (arg, value) => {
          if (value === null) return value;
          return this.validator.string(arg, value);
        }
      }
    );
  }

  default(arg) {
    return new StringTypeConstraint([...this.constraints, {name: 'default', arg}]);
  }

  min(arg) {
    return new StringTypeConstraint([...this.constraints, {name: 'min', arg}]);
  }

  max(arg) {
    return new StringTypeConstraint([...this.constraints, {name: 'max', arg}]);
  }

  length(arg) {
    return new StringTypeConstraint([...this.constraints, {name: 'length', arg}]);
  }

  alphanum() {
    return new StringTypeConstraint([...this.constraints, {name: 'alphanum'}]);
  }

  regex(arg) {
    return new StringTypeConstraint([...this.constraints, {name: 'regex', arg}]);
  }

  regexFlags(arg) {
    return new StringTypeConstraint([...this.constraints, {name: 'regexFlags', arg}]);
  }

  email() {
    return new StringTypeConstraint([...this.constraints, {name: 'email'}]);
  }

  lowercase() {
    return new StringTypeConstraint([...this.constraints, {name: 'lowercase'}]);
  }

  uppercase() {
    return new StringTypeConstraint([...this.constraints, {name: 'uppercase'}]);
  }

  enum(arg) {
    return new StringTypeConstraint([...this.constraints, {name: 'enum', arg}]);
  }

  uuid(arg) {
    return new StringTypeConstraint([...this.constraints, {name: 'uuid', arg}]);
  }
}

let numberValidators = {
  number: (arg, value) => {
    if (typeof value !== 'number') {
      throw new Error('Value must be a number');
    }
    return value;
  },
  min: (arg, value) => {
    if (value < arg) {
      throw new Error(`Value must be less than ${arg}`);
    }
    return value;
  },
  max: (arg, value) => {
    if (value > arg) {
      throw new Error(`Value cannot be greater than ${arg}`);
    }
    return value;
  },
  integer: (arg, value) => {
    if (!Number.isInteger(value)) {
      throw new Error('Value must be an integer');
    }
    return value;
  }
};

class NumberTypeConstraint extends TypeConstraint {
  constructor(constraints, validators) {

  }

  required() {
    return new NumberTypeConstraint([...this.constraints, {name: 'required'}]);
  }

  allowNull(arg) {
    return new NumberTypeConstraint([...this.constraints, {name: 'allowNull', arg}]);
  }

  default(arg) {
    return new NumberTypeConstraint([...this.constraints, {name: 'default', arg}]);
  }

  min(arg) {
    return new NumberTypeConstraint([...this.constraints, {name: 'min', arg}]);
  }

  max(arg) {
    return new NumberTypeConstraint([...this.constraints, {name: 'max', arg}]);
  }

  integer() {
    return new NumberTypeConstraint([...this.constraints, {name: 'integer'}]);
  }
}

let booleanValidators = {
  boolean: (arg, value) => {
    if (typeof value !== 'boolean') {
      throw new Error('Value must be a boolean');
    }
    return value;
  }
};

class BooleanTypeConstraint extends TypeConstraint {
  constructor(constraints, validators) {

  }

  required() {
    return new BooleanTypeConstraint([...this.constraints, {name: 'required'}]);
  }

  allowNull(arg) {
    return new BooleanTypeConstraint([...this.constraints, {name: 'allowNull', arg}]);
  }

  default(arg) {
    return new BooleanTypeConstraint([...this.constraints, {name: 'default', arg}]);
  }
}

let typeBuilder = {
  string: () => {
    return new StringTypeConstraint([{name: 'string'}]);
  },
  number: () => {
    return new NumberTypeConstraint([{name: 'number'}]);
  },
  boolean: () => {
    return new BooleanTypeConstraint([{name: 'boolean'}]);
  }
};

module.exports = {
  validateQuery,
  createVerifier,
  typeBuilder
};
