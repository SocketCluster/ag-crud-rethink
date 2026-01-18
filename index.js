const rethinkdbdash = require('rethinkdbdash');
const AccessController = require('./access-controller');
const Cache = require('./cache');
const AsyncStreamEmitter = require('async-stream-emitter');
const jsonStableStringify = require('json-stable-stringify');
const { constructTransformedRethinkQuery } = require('./query-transformer');
const { validateQuery, createModelValidator, typeBuilder } = require('./validate');
const errors = require('./errors');

let AGCRUDRethink = function (options) {
  AsyncStreamEmitter.call(this);

  this.options = Object.assign({}, options);

  if (!this.options.schema) {
    this.options.schema = {};
  }

  this.modelValidators = {};
  this.schema = this.options.schema;
  this.rethink = rethinkdbdash(this.options.databaseOptions);
  this.options.rethink = this.rethink;
  this.maxErrorCount = this.options.maxErrorCount ?? 100;
  this.maxMultiPublish = this.options.maxMultiPublish ?? 20;

  this.channelPrefix = 'crud>';

  if (!this.options.defaultPageSize) {
    this.options.defaultPageSize = 10;
  }
  
  this._foreignViews = {};
  this._typeRelations = {};

  if (this.options.clientErrorMapper) {
    this.clientErrorMapper = this.options.clientErrorMapper;
  } else {
    this.clientErrorMapper = (error) => error;
  }

  for (let modelName of Object.keys(this.schema)) {
    let modelSchema = this.schema[modelName];
    let modelSchemaViews = modelSchema.views || {};
    let relations = modelSchema.relations || {};

    for (let viewName of Object.keys(modelSchemaViews)) {
      let viewSchema = modelSchemaViews[viewName];
      let paramFields = viewSchema.paramFields || [];

      let foreignAffectingFieldsMap = viewSchema.foreignAffectingFields || {};
      for (let type of Object.keys(foreignAffectingFieldsMap)) {
        if (!this.schema[type]) {
          throw new Error(
            `The ${type} model does not exist so it cannot be declared as a key of foreignAffectingFields on the ${
              viewName
            } view on the ${modelName} model.`
          );
        }

        let modelFields = this.schema[type].fields || {};
        let modelRelations = relations[type] || {};

        for (let fieldName of paramFields) {
          let foreignModelHasParamField = modelFields.hasOwnProperty(fieldName);
          let foreignRelationHasParamField = modelRelations.hasOwnProperty(fieldName);

          if (!foreignModelHasParamField && !foreignRelationHasParamField) {
            throw new Error(
              `The ${type} model does not have a matching ${
                fieldName
              } field so it cannot be used as a foreignAffectingFields model for the ${
                viewName
              } view which is defined on the ${modelName} model. Also, the ${
                fieldName
              } field could not be derived from a relation defined on the ${
                modelName
              } model.`
            );
          }
        }

        let affectingFields = foreignAffectingFieldsMap[type];

        if (!this._foreignViews[type]) {
          this._foreignViews[type] = {};
        }
        if (!this._foreignViews[type][modelName]) {
          this._foreignViews[type][modelName] = {};
        }
        this._foreignViews[type][modelName][viewName] = {
          paramFields,
          affectingFields
        };
      }
    }

    for (let sourceType of Object.keys(relations)) {
      if (!this.schema[sourceType]) {
        throw new Error(
          `The ${sourceType} model does not exist so it cannot be declared as a relation on the ${
            modelName
          } model.`
        );
      }
      let fieldRelations = relations[sourceType];
      if (!this._typeRelations[sourceType]) {
        this._typeRelations[sourceType] = {};
      }
      if (!this._typeRelations[sourceType][modelName]) {
        this._typeRelations[sourceType][modelName] = fieldRelations;
      }
    }

    this.modelValidators[modelName] = createModelValidator(
      modelName,
      modelSchema.fields,
      { maxErrorCount: this.maxErrorCount }
    );
  }

  this.options.modelValidators = this.modelValidators;

  let cacheDisabled;
  if (this.options.agServer) {
    this.agServer = this.options.agServer;
    cacheDisabled = this.options.cacheDisabled || false;
  } else {
    this.agServer = null;
    if (this.options.hasOwnProperty('cacheDisabled')) {
      cacheDisabled = this.options.cacheDisabled || false;
    } else {
      // If agServer is not defined and cacheDisabled isn't defined,
      // then by default we will disable the cache.
      cacheDisabled = true;
    }
  }

  this.cache = new Cache({
    cacheDisabled: cacheDisabled,
    cacheDuration: this.options.cacheDuration
  });
  this.options.cache = this.cache;

  (async () => {
    for await (let {query} of this.cache.listener('expire')) {
      this._cleanupResourceChannel(query);
    }
  })();
  (async () => {
    for await (let {query} of this.cache.listener('clear')) {
      this._cleanupResourceChannel(query);
    }
  })();

  if (this.agServer) {
    this.accessFilter = new AccessController(this.agServer, this.options);

    (async () => {
      for await (let event of this.accessFilter.listener('error')) {
        this.emit('error', event);
      }
    })();

    this.publish = this.agServer.exchange.transmitPublish.bind(this.agServer.exchange);

    (async () => {
      let consumer = this.agServer.listener('handshake').createConsumer();
      while (true) {
        let packet = await consumer.next();
        if (packet.value && packet.value.socket) {
          this._attachSocket(packet.value.socket);
        }
      }
    })();
  } else {
    // If no server is available, publish will be a no-op.
    this.publish = () => {};
  }
};

AGCRUDRethink.prototype = Object.create(AsyncStreamEmitter.prototype);

AGCRUDRethink.prototype.init = async function () {
  let databases = await this.rethink.dbList().run();
  if (!databases.includes(this.options.databaseOptions.db)) {
    await this.rethink.dbCreate(this.options.databaseOptions.db).run();
  }
  let tables = await this.rethink.tableList().run();
  for (let modelName of Object.keys(this.schema)) {
    if (!tables.includes(modelName)) {
      await this.rethink.tableCreate(modelName).run();
    }
    let modelSchema = this.schema[modelName];
    let indexes = modelSchema.indexes || [];
    let activeIndexesSet = new Set(
      await this.rethink.table(modelName).indexList().run()
    );
    let indexesToBuild = modelSchema.indexesToBuild || [];
    let indexesToRebuild = [];
    for (let indexName of indexesToBuild) {
      if (activeIndexesSet.has(indexName)) {
        indexesToRebuild.push(indexName);
      }
    }
    await Promise.all(
      indexesToRebuild.map(async (indexName) => {
        await this.rethink.table(modelName).indexDrop(indexName).run();
      })
    );
    let indexesToBuildSet = new Set(indexesToBuild);
    await Promise.all(
      indexes.map(async (indexData) => {
        if (typeof indexData === 'string') {
          if (!activeIndexesSet.has(indexData) || indexesToBuildSet.has(indexData)) {
            await this.rethink.table(modelName).indexCreate(indexData).run();
          }
        } else {
          if (!indexData.name) {
            throw new Error(
              `One of the indexes for the ${
                modelName
              } model schema was invalid. Each index must either be a string or an object with a name property. If it is an object, it may also specify optional fn and options properties.`
            );
          }
          if (!activeIndexesSet.has(indexData.name) || indexesToBuildSet.has(indexData.name)) {
            if (indexData.type === 'compound') {
              await this.rethink.table(modelName).indexCreate(indexData.name, indexData.fn(this.rethink)).run();
            } else {
              await this.rethink.table(modelName).indexCreate(indexData.name, indexData.fn, indexData.options).run();
            }
          }
        }
      })
    );
  }
};

AGCRUDRethink.prototype._getResourceChannelName = function (resource) {
  return this.channelPrefix + resource.type + '/' + resource.id;
};

AGCRUDRethink.prototype._getResourcePropertyChannelName = function (resourceProperty) {
  return this.channelPrefix + resourceProperty.type + '/' + resourceProperty.id + '/' + resourceProperty.field;
};

AGCRUDRethink.prototype._cleanupResourceChannel = function (resource) {
  let resourceChannelName = this._getResourceChannelName(resource);
  let resourceChannel = this.agServer.exchange.channel(resourceChannelName);
  resourceChannel.unsubscribe();
  resourceChannel.kill();
};

AGCRUDRethink.prototype._handleResourceChange = function (resource) {
  this.cache.clear(resource);
};

AGCRUDRethink.prototype._mapResourceField = function (fieldName, resource, sourceType, targetType) {
  if (
    this._typeRelations[sourceType] &&
    this._typeRelations[sourceType][targetType] &&
    this._typeRelations[sourceType][targetType][fieldName]
  ) {
    let relationFn = this._typeRelations[sourceType][targetType][fieldName];
    return {
      success: true,
      value: relationFn(resource)
    };
  }
  return {
    success: false
  };
};

AGCRUDRethink.prototype._getForeignViews = function (type) {
  return this._foreignViews[type] || {};
};

AGCRUDRethink.prototype._getViews = function (type) {
  let typeSchema = this.schema[type] || {};
  return typeSchema.views || {};
};

AGCRUDRethink.prototype._isValidView = function (type, viewName) {
  let modelViews = this._getViews(type);
  return modelViews.hasOwnProperty(viewName);
};

AGCRUDRethink.prototype._getView = function (type, viewName) {
  let modelViews = this._getViews(type);
  return modelViews[viewName];
};

AGCRUDRethink.prototype._getViewChannelName = function (viewName, viewParams, type) {
  let primaryParams;
  let viewSchema = this._getView(type, viewName);

  if (viewSchema && viewSchema.primaryFields) {
    primaryParams = {};

    for (let field of viewSchema.primaryFields) {
      primaryParams[field] = viewParams[field] === undefined ? null : viewParams[field];
    }
  } else {
    primaryParams = viewParams || {};
  }
  if (!this.options.typedViewChannelParams) {
    for (let [key, value] of Object.entries(primaryParams)) {
      primaryParams[key] = String(value);
    }
  }
  let viewPrimaryParamsString = jsonStableStringify(primaryParams);
  return this.channelPrefix + viewName + '(' + viewPrimaryParamsString + '):' + type;
};

AGCRUDRethink.prototype._areObjectsEqual = function (objectA, objectB) {
  let objectStringA = jsonStableStringify(objectA || {});
  let objectStringB = jsonStableStringify(objectB || {});
  return objectStringA === objectStringB;
};

AGCRUDRethink.prototype._isModelFieldMulti = function (type, field) {
  return this.schema[type]?.fields?.[field]?.options?.multi || false;
};

AGCRUDRethink.prototype._publishToViewChannel = function (viewData, operation, otherViewData) {
  let viewSchema = this._getView(viewData.type, viewData.view);
  if (!viewSchema || viewSchema.disableRealtime) return;
  let paramsVariants = [];
  let params = viewData.params;
  
  let otherParams = otherViewData?.params || {};
  let otherMultiParams = {};
  for (let [field, value] of Object.entries(otherParams)) {
    if (typeof value !== 'string') continue;
    if (this._isModelFieldMulti(viewData.type, field)) {
      otherMultiParams[field] = Object.fromEntries(
        value.split(',').map(
          (value, i) => [i, {[value.trim()]: true}]
        )
      );
    }
  }
  if (params) {
    paramsVariants.push(params);
  } else {
    params = {};
  }
  for (let [field, value] of Object.entries(params)) {
    if (value == null) {
      let paramsClone = {...params};
      paramsClone[field] = 'false';
      paramsVariants.push(paramsClone);
    }
    if (typeof value !== 'string') continue;
    if (this._isModelFieldMulti(viewData.type, field)) {
      let multiParts = value.split(',');
      if (multiParts.length > 1) {
        let i = 0;
        for (let part of multiParts) {
          let paramsClone = {...params};
          let value = part.trim();
          if (!otherMultiParams[field]?.[i]?.[value]) {
            paramsClone[field] = value;
            paramsVariants.push(paramsClone);
          }
          i++;
        }
      }
    }
  }
  for (let i = 0; i < paramsVariants.length; i++) {
    if (i > this.maxMultiPublish) break;
    let viewParams = paramsVariants[i];
    let viewChannelName = this._getViewChannelName(viewData.view, viewParams, viewData.type);
    if (operation === undefined) {
      this.publish(viewChannelName);
    } else {
      this.publish(viewChannelName, operation);
    }
  }
};

AGCRUDRethink.prototype._publishViewUpdates = async function (query, newResource, oldResource) {
  let oldAffectedViewData = this.getQueryAffectedViews(query, oldResource);

  let oldViewDataMap = {};
  for (let viewData of oldAffectedViewData) {
    oldViewDataMap[`${viewData.view}:${viewData.type}`] = viewData;
  }

  let newAffectedViewData = this.getQueryAffectedViews(query, newResource);

  for (let viewData of newAffectedViewData) {
    let oldViewData = oldViewDataMap[`${viewData.view}:${viewData.type}`] || {};
    let areViewParamsEqual = this._areObjectsEqual(oldViewData.params, viewData.params);

    if (areViewParamsEqual) {
      let areAffectingDataEqual = this._areObjectsEqual(oldViewData.affectingData, viewData.affectingData);

      if (!areAffectingDataEqual) {
        this._publishToViewChannel(viewData, {
          type: 'update',
          value: {
            id: query.id
          }
        });
      }
    } else {
      this._publishToViewChannel(oldViewData, {
        type: 'update',
        value: {
          id: query.id
        }
      }, viewData);
      this._publishToViewChannel(viewData, {
        type: 'update',
        value: {
          id: query.id
        }
      }, oldViewData);
    }
  }
};

AGCRUDRethink.prototype.getModifiedResourceFields = function (updateDetails) {
  let oldResource = updateDetails.oldResource || {};
  let newResource = updateDetails.newResource || {};
  let modifiedFieldsMap = {};

  for (let fieldName of Object.keys(oldResource)) {
    if (oldResource[fieldName] !== newResource[fieldName]) {
      modifiedFieldsMap[fieldName] = {before: oldResource[fieldName], after: newResource[fieldName]};
    }
  }
  for (let fieldName of Object.keys(newResource)) {
    if (!modifiedFieldsMap.hasOwnProperty(fieldName) && newResource[fieldName] !== oldResource[fieldName]) {
      modifiedFieldsMap[fieldName] = {before: oldResource[fieldName], after: newResource[fieldName]};
    }
  }

  return modifiedFieldsMap;
};

AGCRUDRethink.prototype.getQueryAffectedViews = function (query, resource) {
  let updateDetails = {
    type: query.type,
    resource
  };
  if (query.field) {
    updateDetails.fields = [query.field];
  }
  return this.getAffectedViews(updateDetails);
};

AGCRUDRethink.prototype.getAffectedViews = function (updateDetails) {
  let affectedViews = [];
  let resource = updateDetails.resource || {};

  let viewSchemaMap = this._getViews(updateDetails.type);
  let foreignViewsMap = this._getForeignViews(updateDetails.type);

  let allViewSchemas = Object.keys(viewSchemaMap)
  .map((viewName) => {
    return {
      name: viewName,
      type: updateDetails.type,
      schema: viewSchemaMap[viewName]
    };
  })
  .concat(
    Object.keys(foreignViewsMap).flatMap((parentType) => {
      let viewSchemaMap = foreignViewsMap[parentType];
      return Object.keys(viewSchemaMap).map((viewName) => {
        let viewSchema = viewSchemaMap[viewName];
        return {
          name: viewName,
          type: parentType,
          schema: viewSchema
        };
      });
    })
  );

  for (let viewData of allViewSchemas) {
    let viewName = viewData.name;
    let viewSchema = viewData.schema;
    let paramFields = viewSchema.paramFields || [];
    let affectingFields = viewSchema.affectingFields || [];

    let params = {};
    let affectingData = {};

    for (let fieldName of paramFields) {
      let {success, value} = this._mapResourceField(fieldName, resource, updateDetails.type, viewData.type);
      if (success) {
        params[fieldName] = value;
        affectingData[fieldName] = value;
      } else {
        params[fieldName] = resource[fieldName];
        affectingData[fieldName] = resource[fieldName];
      }
    }

    for (let fieldName of affectingFields) {
      let {success, value} = this._mapResourceField(fieldName, resource, updateDetails.type, viewData.type);
      if (success) {
        affectingData[fieldName] = value;
      } else {
        affectingData[fieldName] = resource[fieldName];
      }
    }

    if (updateDetails.fields) {
      let updatedFields = updateDetails.fields;
      let isViewAffectedByUpdate = false;

      let affectingFieldsLookup = {
        id: true
      };
      for (let fieldName of paramFields) {
        affectingFieldsLookup[fieldName] = true;
      }
      for (let fieldName of affectingFields) {
        affectingFieldsLookup[fieldName] = true;
      }

      let modifiedFieldsLength = updatedFields.length;
      for (let i = 0; i < modifiedFieldsLength; i++) {
        let fieldName = updatedFields[i];
        if (affectingFieldsLookup[fieldName]) {
          isViewAffectedByUpdate = true;
          break;
        }
      }

      if (isViewAffectedByUpdate) {
        affectedViews.push({
          view: viewName,
          type: viewData.type,
          params,
          affectingData
        });
      }
    } else {
      affectedViews.push({
        view: viewName,
        type: viewData.type,
        params,
        affectingData
      });
    }
  }
  return affectedViews;
};

AGCRUDRethink.prototype._updateDb = async function (type, id, record) {
  let result = await this.rethink.table(type).get(id).update(record, {returnChanges: true}).run();
  if (result.errors) {
    throw errors.create(result.first_error);
  }
  if (!result.changes.length) {
    return {};
  }
  return result.changes[0].new_val;
};

/*
  If you update the database outside of ag-crud-rethink, you can use this method
  to clear ag-crud-rethink cache for a resource and notify all client subscribers
  about the change.

  The updateDetails argument must be an object with the following properties:
    type: The resource type which was updated (name of the collection).
    id: The id of the specific resource/document which was updated.
    fields: Fields which were updated within the resource - Can be either
      an array of field names or an object where each key represents a field name
      and each value represents the new updated value for the field (providing
      updated values is a performance optimization).
*/
AGCRUDRethink.prototype.notifyResourceUpdate = function (updateDetails) {
  if (updateDetails == null) {
    let invalidArgumentsError = new Error('The updateDetails object was not specified');
    invalidArgumentsError.name = 'InvalidArgumentsError';
    throw invalidArgumentsError;
  }
  if (updateDetails.type === undefined) {
    let invalidArgumentsError = new Error('The updateDetails object did not have a type property');
    invalidArgumentsError.name = 'InvalidArgumentsError';
    throw invalidArgumentsError;
  }
  if (updateDetails.id === undefined) {
    let invalidArgumentsError = new Error('The updateDetails object did not have an id property');
    invalidArgumentsError.name = 'InvalidArgumentsError';
    throw invalidArgumentsError;
  }
  if (updateDetails.fields === undefined) {
    let invalidArgumentsError = new Error('The updateDetails object did not have a fields property');
    invalidArgumentsError.name = 'InvalidArgumentsError';
    throw invalidArgumentsError;
  }

  let resourceChannelName = this._getResourceChannelName(updateDetails);
  // This will cause the resource cache to clear itself.
  this.publish(resourceChannelName);

  let updatedFields = updateDetails.fields || [];
  if (Array.isArray(updatedFields)) {
    for (let fieldName of updatedFields) {
      let resourcePropertyChannelName = this._getResourcePropertyChannelName({
        type: updateDetails.type,
        id: updateDetails.id,
        field: fieldName
      });
      // Notify individual field subscribers about the change.
      this.publish(resourcePropertyChannelName);
    }
  } else {
    // Notify individual field subscribers about the change and provide the new value.
    for (let fieldName of Object.keys(updatedFields)) {
      let resourcePropertyChannelName = this._getResourcePropertyChannelName({
        type: updateDetails.type,
        id: updateDetails.id,
        field: fieldName
      });
      let fieldValue = updatedFields[fieldName];
      if (typeof fieldValue === 'function') {
        // Do not publish raw RethinkDB predicates or functions.
        this.publish(resourcePropertyChannelName);
      } else {
        this.publish(resourcePropertyChannelName, {
          type: 'update',
          value: fieldValue
        });
      }
    }
  }
};

/*
  If you update the database outside of ag-crud-rethink, you can use this method
  to clear ag-crud-rethink cache for a view and notify all client subscribers
  about the change.

  The updateDetails argument must be an object with the following properties:
    type: The resource type which was updated (name of the collection).
    view: The name of the view.
    params: The predicate object/value which defines the affected view.
*/
AGCRUDRethink.prototype.notifyViewUpdate = function (updateDetails, operation) {
  if (updateDetails == null) {
    let invalidArgumentsError = new Error('The updateDetails object was not specified');
    invalidArgumentsError.name = 'InvalidArgumentsError';
    throw invalidArgumentsError;
  }
  if (updateDetails.type === undefined) {
    let invalidArgumentsError = new Error('The updateDetails object did not have a type property');
    invalidArgumentsError.name = 'InvalidArgumentsError';
    throw invalidArgumentsError;
  }
  if (updateDetails.view === undefined) {
    let invalidArgumentsError = new Error('The updateDetails object did not have a view property');
    invalidArgumentsError.name = 'InvalidArgumentsError';
    throw invalidArgumentsError;
  }
  if (updateDetails.params === undefined) {
    let invalidArgumentsError = new Error('The updateDetails object did not have a params property');
    invalidArgumentsError.name = 'InvalidArgumentsError';
    throw invalidArgumentsError;
  }
  this._publishToViewChannel(updateDetails, operation);
};

/*
  If you update the database outside of ag-crud-rethink, you can use this method
  to clear ag-crud-rethink cache and notify all client subscribers (both the resource
  and any affected views) about the change.

  The updateDetails argument must be an object with the following properties:
    type: The resource type which was updated (name of the collection).
    oldResource: The old document/resource before the update was made.
      If the resource did not exist before (newly created), then this
      should be set to null.
    newResource: The new document/resource after the update was made.
      If the resource no longer exists after the operation (deleted), then
      this should be set to null.
*/
AGCRUDRethink.prototype.notifyUpdate = function (updateDetails) {
  if (updateDetails == null) {
    let invalidArgumentsError = new Error('The updateDetails object was not specified');
    invalidArgumentsError.name = 'InvalidArgumentsError';
    throw invalidArgumentsError;
  }
  if (updateDetails.type === undefined) {
    let invalidArgumentsError = new Error('The updateDetails object did not have a type property');
    invalidArgumentsError.name = 'InvalidArgumentsError';
    throw invalidArgumentsError;
  }
  if (updateDetails.oldResource === undefined && updateDetails.newResource === undefined) {
    let invalidArgumentsError = new Error('The updateDetails object did not have either an oldResource or newResource property');
    invalidArgumentsError.name = 'InvalidArgumentsError';
    throw invalidArgumentsError;
  }

  let refResource = updateDetails.oldResource || updateDetails.newResource || {};
  let oldResource = updateDetails.oldResource || {};
  let newResource = updateDetails.newResource || {};

  let updatedFieldsMap = this.getModifiedResourceFields(updateDetails);
  let updatedFieldsList = Object.keys(updatedFieldsMap);

  if (!updatedFieldsList.length) {
    return;
  }

  this.notifyResourceUpdate({
    type: updateDetails.type,
    id: refResource.id,
    fields: updatedFieldsList
  });

  let oldViewParamsMap = {};
  let oldResourceAffectedViews = this.getAffectedViews({
    type: updateDetails.type,
    resource: oldResource,
    fields: updatedFieldsList
  });

  let newResourceAffectedViews = this.getAffectedViews({
    type: updateDetails.type,
    resource: newResource,
    fields: updatedFieldsList
  });

  for (let viewData of oldResourceAffectedViews) {
    oldViewParamsMap[viewData.view] = viewData.params;
  }

  for (let viewData of oldResourceAffectedViews) {
    let operation;
    if (updateDetails.newResource == null) {
      operation = {
        type: 'delete',
        value: {
          id: refResource.id
        }
      };
    } else {
      operation = {
        type: 'update',
        value: {
          id: refResource.id
        }
      };
    }
    this.notifyViewUpdate({
      type: viewData.type,
      view: viewData.view,
      params: viewData.params
    }, operation);
  }

  for (let viewData of newResourceAffectedViews) {
    if (!this._areObjectsEqual(oldViewParamsMap[viewData.view], viewData.params)) {
      let operation;
      if (updateDetails.oldResource == null) {
        operation = {
          type: 'create',
          value: {
            id: refResource.id
          }
        };
      } else {
        operation = {
          type: 'update',
          value: {
            id: refResource.id
          }
        };
      }
      this.notifyViewUpdate({
        type: viewData.type,
        view: viewData.view,
        params: viewData.params
      }, operation);
    }
  }
};

// Add a new document to a collection. This will send a change notification to each
// affected view (taking into account the affected page number within each view).
// This allows views to update themselves on the front-end in real-time.
AGCRUDRethink.prototype.create = async function (query, socket) {
  this._validateQuery({action: 'create', ...query});
  return this._create(query, socket);
};

AGCRUDRethink.prototype._create = async function (query, socket) {
  let modelValidator = this.modelValidators[query.type];

  try {
    if (modelValidator == null) {
      let error = new Error(`The ${query.type} model type is not supported - It is not part of the schema`);
      error.name = 'CRUDInvalidModelType';
      throw error;
    }
    if (!query.value || typeof query.value !== 'object') {
      let error = new Error('Cannot create a document from a primitive - Must be an object');
      error.name = 'CRUDInvalidParams';
      throw error;
    }
    query.value = modelValidator(query.value);

    let result = await this.rethink.table(query.type)
      .insert(query.value, {returnChanges: true})
      .run();
    if (result.errors) {
      throw errors.create(result.first_error);
    }
    result = result.changes[0].new_val;
    let resourceChannelName = this._getResourceChannelName({
      type: query.type,
      id: result.id
    });
    this.publish(resourceChannelName);

    let affectedViewData = this.getQueryAffectedViews(query, result);
    for (let viewData of affectedViewData) {
      this._publishToViewChannel(viewData, {
        type: 'create',
        value: {
          id: result.id
        }
      });
    }
    this.emit('create', {query, result});

    return result.id;

  } catch (error) {
    this.emit('error', {error});
    this.emit('createFail', {query, error});
    throw error;
  }
};

// Read either a collection of IDs, a single document or a single field
// within a document. To achieve efficient field-level granularity, a cache is used.
// A cache entry will automatically get cleared when ag-crud-rethink detects
// a real-time change to a field which is cached.
AGCRUDRethink.prototype.read = async function (query, socket) {
  this._validateQuery({action: 'read', ...query});
  return this._read(query, socket);
};

AGCRUDRethink.prototype._read = async function (query, socket) {
  let pageSize = query.pageSize ?? this.options.defaultPageSize;
  let modelValidator = this.modelValidators[query.type];
  if (modelValidator == null) {
    let error = new Error(`The ${query.type} model type is not supported - It is not part of the schema`);
    error.name = 'CRUDInvalidModelType';
    throw error;
  }

  let data;
  let count;

  if (query.id) {
    let resourceChannelName = this._getResourceChannelName(query);
    let isSubscribedToResourceChannel = this.agServer.exchange.isSubscribed(resourceChannelName, true);

    if (!isSubscribedToResourceChannel) {
      let resourceChannel = this.agServer.exchange.subscribe(resourceChannelName);
      (async () => {
        for await (let resourceData of resourceChannel) {
          this._handleResourceChange(query);
        }
      })();
    }

    data = await this.cache.pass(query, async () => {
      return await this.rethink.table(query.type).get(query.id).run();
    });
  } else {
    let rethinkQuery = constructTransformedRethinkQuery(this.options, this.rethink.table(query.type), query.type, query.view, query.viewParams);

    let tasks = [];

    if (query.offset) {
      tasks.push(
        rethinkQuery.slice(query.offset, query.offset + pageSize + 1).pluck('id').run()
      );
    } else {
      tasks.push(
        rethinkQuery.limit(pageSize + 1).pluck('id').run()
      );
    }
    if (query.getCount) {
      tasks.push(
        rethinkQuery.count().run()
      );
    }

    let results = await Promise.all(tasks);
    data = results[0];
    count = results[1];
  }

  // If socket does not exist, then the CRUD operation comes from the server-side
  // and we don't need to pass it through an accessFilter.
  let applyPostAccessFilter;
  if (socket && this.accessFilter) {
    applyPostAccessFilter = this.accessFilter.applyPostAccessFilter.bind(this.accessFilter);
  } else {
    applyPostAccessFilter = () => Promise.resolve();
  }
  let accessFilterRequest = {
    r: this.rethink,
    socket,
    action: 'read',
    authToken: socket && socket.authToken,
    query,
    resource: data
  };

  await applyPostAccessFilter(accessFilterRequest);

  let result;
  if (query.id) {
    if (query.field) {
      if (data == null) {
        data = {};
      }
      result = data[query.field];
      if (typeof result === 'string' && query.sliceTo != null) {
        result = result.slice(0, query.sliceTo);
      }
    } else {
      result = data;
    }
  } else {
    let documentList = [];
    let resultCount = Math.min(data.length, pageSize);

    for (let i = 0; i < resultCount; i++) {
      documentList.push(data[i].id || null);
    }
    result = {
      data: documentList
    };

    if (query.getCount) {
      result.count = count;
    }

    if (data.length < pageSize + 1) {
      result.isLastPage = true;
    }
  }
  // Return null instead of undefined - That way the frontend will know
  // that the value was read but didn't exist (or was null).
  if (result === undefined) {
    return null;
  }

  return result;
};

// Update a single whole document or one or more fields within a document.
// Whenever a document is updated, it may affect the ordering and pagination of
// certain views. This update operation will send notifications to all affected
// clients to let them know if a view that they are currently looking at
// has been affected by the update operation - This allows them to update
// themselves in real-time.
AGCRUDRethink.prototype.update = async function (query, socket) {
  this._validateQuery({action: 'update', ...query});
  return this._update(query, socket);
};

AGCRUDRethink.prototype._update = async function (query, socket) {
  let modelValidator = this.modelValidators[query.type];

  try {
    if (modelValidator == null) {
      let error = new Error(`The ${query.type} model type is not supported - It is not part of the schema`);
      error.name = 'CRUDInvalidModelType';
      throw error;
    }
    if (query.id == null) {
      let error = new Error('Cannot update document without specifying an id');
      error.name = 'CRUDInvalidParams';
      throw error;
    }

    // If socket does not exist, then the CRUD operation comes from the server-side
    // and we don't need to pass it through a accessFilter.
    let applyPostAccessFilter;
    if (socket && this.accessFilter) {
      applyPostAccessFilter = this.accessFilter.applyPostAccessFilter.bind(this.accessFilter);
    } else {
      applyPostAccessFilter = () => Promise.resolve();
    }

    let modelInstance;

    if (query.field) {
      if (query.field === 'id') {
        let error = new Error('Cannot modify the id field of an existing document');
        error.name = 'CRUDInvalidOperation';
        throw error;
      }
      modelInstance = await this.rethink.table(query.type).get(query.id).run();
    } else {
      if (!query.value || typeof query.value !== 'object') {
        let error = new Error('Cannot replace document with a primitive - Must be an object');
        error.name = 'CRUDInvalidOperation';
        throw error;
      }
      modelInstance = await this.rethink.table(query.type).get(query.id).run();
    }

    let modelInstanceClone = {...modelInstance};

    let accessFilterRequest = {
      r: this.rethink,
      socket,
      action: 'update',
      authToken: socket && socket.authToken,
      query,
      resource: modelInstance
    };

    await applyPostAccessFilter(accessFilterRequest);

    let queryValue;

    if (query.field) {
      queryValue = modelValidator({[query.field]: query.value}, true, true);
    } else {
      queryValue = modelValidator(query.value, true, true);
    }

    let result = await this._updateDb(query.type, query.id, queryValue);
    this.cache.update(query);

    let resourceChannelName = this._getResourceChannelName(query);
    this.publish(resourceChannelName);

    let publisherId = typeof query.publisherId === 'string' ? query.publisherId : undefined;

    if (query.field) {
      if (queryValue === undefined) {
        queryValue = null;
      }
      if (typeof queryValue === 'function') {
        // Do not publish raw RethinkDB predicates or functions.
        this.publish(this.channelPrefix + query.type + '/' + query.id + '/' + query.field);
      } else {
        this.publish(this.channelPrefix + query.type + '/' + query.id + '/' + query.field, {
          type: 'update',
          value: queryValue,
          publisherSocketId: socket && socket.id,
          publisherId
        });
      }
    } else {
      queryValue = queryValue || {};
      for (let field of Object.keys(queryValue)) {
        let value = queryValue[field];
        if (value === undefined) {
          value = null;
        }
        if (typeof value === 'function') {
          // Do not publish raw RethinkDB predicates or functions.
          this.publish(this.channelPrefix + query.type + '/' + query.id + '/' + field);
        } else {
          this.publish(this.channelPrefix + query.type + '/' + query.id + '/' + field, {
            type: 'update',
            value,
            publisherSocketId: socket && socket.id,
            publisherId
          });
        }
      }
    }
    this._publishViewUpdates(query, result, modelInstanceClone);
    this.emit('update', {query, result});

  } catch (error) {
    this.emit('error', {error});
    this.emit('updateFail', {query, error});
    throw error;
  }
};

// Delete a single document or field from a document.
// This will notify affected views so that they may update themselves
// in real-time.
AGCRUDRethink.prototype.delete = async function (query, socket) {
  this._validateQuery({action: 'delete', ...query});
  return this._delete(query, socket);
};

AGCRUDRethink.prototype._delete = async function (query, socket) {
  let modelValidator = this.modelValidators[query.type];

  try {
    if (modelValidator == null) {
      let error = new Error(`The ${query.type} model type is not supported - It is not part of the schema`);
      error.name = 'CRUDInvalidModelType';
      throw error;
    }
  } catch (error) {
    this.emit('error', {error});
    this.emit('deleteFail', {query, error});
    throw error;
  }

  if (!query.id) {
    let error = new Error('Cannot delete an entire collection - ID must be provided');
    error.name = 'CRUDInvalidParams';
    throw error;
  }

  // If socket does not exist, then the CRUD operation comes from the server-side
  // and we don't need to pass it through a accessFilter.
  let applyPostAccessFilter;
  if (socket && this.accessFilter) {
    applyPostAccessFilter = this.accessFilter.applyPostAccessFilter.bind(this.accessFilter);
  } else {
    applyPostAccessFilter = () => Promise.resolve();
  }

  let modelInstance = await this.rethink.table(query.type).get(query.id).run();
  let modelInstanceClone = {...modelInstance};

  let accessFilterRequest = {
    r: this.rethink,
    socket,
    action: 'delete',
    authToken: socket && socket.authToken,
    query,
    resource: modelInstance
  };

  await applyPostAccessFilter(accessFilterRequest);

  let result;
  if (query.field) {
    modelValidator({[query.field]: undefined}, true, true);
    result = await this.rethink.table(query.type).get(query.id)
      .replace(
        (row) => {
          return row.without(query.field);
        },
        {returnChanges: true}
      )
      .run();
    if (result.errors) {
      throw errors.create(result.first_error);
    }
    result = result.changes.length ? result.changes[0].new_val : {};
  } else {
    result = await this.rethink.table(query.type)
      .get(query.id).delete({returnChanges: true}).run();
    if (result.errors) {
      throw errors.create(result.first_error);
    }
    result = result.changes.length ? result.changes[0].new_val : {};
  }

  let resourceChannelName = this._getResourceChannelName(query);
  this.publish(resourceChannelName);

  let publisherId = typeof query.publisherId === 'string' ? query.publisherId : undefined;

  if (query.field) {
    this._publishViewUpdates(query, result, modelInstanceClone);
    this.publish(this.channelPrefix + query.type + '/' + query.id + '/' + query.field, {
      type: 'delete',
      publisherSocketId: socket && socket.id,
      publisherId
    });
  } else {
    let oldAffectedViewData = this.getQueryAffectedViews(query, modelInstanceClone);
    for (let viewData of oldAffectedViewData) {
      this._publishToViewChannel(viewData, {
        type: 'delete',
        value: {
          id: query.id
        }
      });
    }

    let modelSchema = this.schema[query.type];
    let deletedFields = Object.keys(modelSchema?.fields || {});

    for (let field of deletedFields) {
      this.publish(this.channelPrefix + query.type + '/' + query.id + '/' + field, {
        type: 'delete',
        publisherSocketId: socket && socket.id,
        publisherId
      });
    }
  }
  this.emit('delete', {query, result});
};

AGCRUDRethink.prototype._attachSocket = function (socket) {
  let actionHandlers = {
    create: async (query) => {
      return this._create(query, socket);
    },
    read: async (query) => {
      return this._read(query, socket);
    },
    update: async (query) => {
      return this._update(query, socket);
    },
    delete: async (query) => {
      return this._delete(query, socket);
    }
  };

  // The combined stream ensures that different events are fully processed
  // in the same order as they arrive.
  (async () => {
    for await (let request of socket.procedure('crud')) {
      let {action, ...query} = request?.data || {};
      let result;
      try {
        result = await actionHandlers[action](query);
      } catch (error) {
        request.error(
          this.clientErrorMapper(error, action, query)
        );
        continue;
      }
      request.end(result);
    }
  })();
};

AGCRUDRethink.prototype._validateQuery = function (query) {
  validateQuery(query, this.schema);
};

module.exports.AGCRUDRethink = AGCRUDRethink;

module.exports.type = typeBuilder;

module.exports.createModelValidator = createModelValidator;

module.exports.attach = function (agServer, options) {
  if (options) {
    options.agServer = agServer;
  } else {
    options = {agServer};
  }
  return new AGCRUDRethink(options);
};
