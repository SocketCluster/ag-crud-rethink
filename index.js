const rethinkdbdash = require('rethinkdbdash');
const async = require('async');
const AccessController = require('./access-controller');
const Cache = require('./cache');
const AsyncStreamEmitter = require('async-stream-emitter');
const jsonStableStringify = require('json-stable-stringify');
const {constructTransformedRethinkQuery} = require('./query-transformer');
const {validateQuery, createModelValidator, typeBuilder} = require('./validate');
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

  Object.keys(this.schema).forEach((modelName) => {
    let modelSchema = this.schema[modelName];
    let modelSchemaViews = modelSchema.views || {};
    let relations = modelSchema.relations || {};

    Object.keys(modelSchemaViews).forEach((viewName) => {
      let viewSchema = modelSchemaViews[viewName];
      let paramFields = viewSchema.paramFields || [];

      let foreignAffectingFieldsMap = viewSchema.foreignAffectingFields || {};
      Object.keys(foreignAffectingFieldsMap).forEach((type) => {
        if (!this.schema[type]) {
          throw new Error(
            `The ${type} model does not exist so it cannot be declared as a key of foreignAffectingFields on the ${
              viewName
            } view on the ${modelName} model.`
          );
        }

        let modelFields = this.schema[type].fields || {};
        let modelRelations = relations[type] || {};

        paramFields.forEach((fieldName) => {
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
        });

        let affectingFields = foreignAffectingFieldsMap[type];

        if (!this._foreignViews[type]) {
          this._foreignViews[type] = {};
        }
        if (!this._foreignViews[type][viewName]) {
          this._foreignViews[type][viewName] = {
            paramFields,
            affectingFields,
            parentType: modelName
          };
        }
      });
    });

    Object.keys(relations).forEach((sourceType) => {
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
    });

    this.modelValidators[modelName] = createModelValidator(modelName, modelSchema.fields);
  });

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

  this._resourceReadBuffer = {};

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
    await Promise.all(
      indexes.map(async (indexData) => {
        if (typeof indexData === 'string') {
          if (!activeIndexesSet.has(indexData)) {
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
          if (!activeIndexesSet.has(indexData.name)) {
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
};

AGCRUDRethink.prototype._handleResourceChange = function (resource) {
  this.cache.clear(resource);
};

AGCRUDRethink.prototype._mapResourceField = function (fieldName, fieldValue, sourceType, targetType) {
  if (sourceType === targetType) {
    return {
      success: true,
      value: fieldValue
    };
  }

  if (
    this._typeRelations[sourceType] &&
    this._typeRelations[sourceType][targetType] &&
    this._typeRelations[sourceType][targetType][fieldName]
  ) {
    let relationFn = this._typeRelations[sourceType][targetType][fieldName];
    return {
      success: true,
      value: relationFn(fieldValue)
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

  if (viewSchema && viewSchema.primaryKeys) {
    primaryParams = {};

    viewSchema.primaryKeys.forEach((field) => {
      primaryParams[field] = viewParams[field] === undefined ? null : viewParams[field];
    });
  } else {
    primaryParams = viewParams || {};
  }

  let viewPrimaryParamsString = jsonStableStringify(primaryParams);
  return this.channelPrefix + viewName + '(' + viewPrimaryParamsString + '):' + type;
};

AGCRUDRethink.prototype._areObjectsEqual = function (objectA, objectB) {
  let objectStringA = jsonStableStringify(objectA || {});
  let objectStringB = jsonStableStringify(objectB || {});
  return objectStringA === objectStringB;
};

AGCRUDRethink.prototype.getModifiedResourceFields = function (updateDetails) {
  let oldResource = updateDetails.oldResource || {};
  let newResource = updateDetails.newResource || {};
  let modifiedFieldsMap = {};

  Object.keys(oldResource).forEach((fieldName) => {
    if (oldResource[fieldName] !== newResource[fieldName]) {
      modifiedFieldsMap[fieldName] = {before: oldResource[fieldName], after: newResource[fieldName]};
    }
  });
  Object.keys(newResource).forEach((fieldName) => {
    if (!modifiedFieldsMap.hasOwnProperty(fieldName) && newResource[fieldName] !== oldResource[fieldName]) {
      modifiedFieldsMap[fieldName] = {before: oldResource[fieldName], after: newResource[fieldName]};
    }
  });

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
    Object.keys(foreignViewsMap).map((viewName) => {
      let viewSchema = foreignViewsMap[viewName];
      return {
        name: viewName,
        type: viewSchema.parentType,
        schema: viewSchema
      };
    })
  );

  allViewSchemas.forEach((viewData) => {
    let viewName = viewData.name;
    let viewSchema = viewData.schema;
    let paramFields = viewSchema.paramFields || [];
    let affectingFields = viewSchema.affectingFields || [];

    let params = {};
    let affectingData = {};

    paramFields.forEach((fieldName) => {
      let {success, value} = this._mapResourceField(resource, updateDetails.type, viewData.type);
      if (success) {
        params[fieldName] = value;
        affectingData[fieldName] = value;
      } else {
        params[fieldName] = resource[fieldName];
        affectingData[fieldName] = resource[fieldName];
      }
    });
    affectingFields.forEach((fieldName) => {
      let {success, value} = this._mapResourceField(resource, updateDetails.type, viewData.type);
      if (success) {
        affectingData[fieldName] = value;
      } else {
        affectingData[fieldName] = resource[fieldName];
      }
    });

    if (updateDetails.fields) {
      let updatedFields = updateDetails.fields;
      let isViewAffectedByUpdate = false;

      let affectingFieldsLookup = {
        id: true
      };
      paramFields.forEach((fieldName) => {
        affectingFieldsLookup[fieldName] = true;
      });
      affectingFields.forEach((fieldName) => {
        affectingFieldsLookup[fieldName] = true;
      });

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
  });
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
    updatedFields.forEach((fieldName) => {
      let resourcePropertyChannelName = this._getResourcePropertyChannelName({
        type: updateDetails.type,
        id: updateDetails.id,
        field: fieldName
      });
      // Notify individual field subscribers about the change.
      this.publish(resourcePropertyChannelName);
    });
  } else {
    // Notify individual field subscribers about the change and provide the new value.
    Object.keys(updatedFields).forEach((fieldName) => {
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
    });
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
  let viewChannelName = this._getViewChannelName(
    updateDetails.view,
    updateDetails.params,
    updateDetails.type
  );
  if (operation === undefined) {
    this.publish(viewChannelName);
  } else {
    this.publish(viewChannelName, operation);
  }
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

  let newViewDataMap = {};
  let newResourceAffectedViews = this.getAffectedViews({
    type: updateDetails.type,
    resource: newResource,
    fields: updatedFieldsList
  });

  newResourceAffectedViews.forEach((viewData) => {
    newViewDataMap[viewData.view] = viewData;
  });

  oldResourceAffectedViews.forEach((viewData) => {
    oldViewParamsMap[viewData.view] = viewData.params;
  });

  oldResourceAffectedViews.forEach((viewData) => {
    let operation;
    if (updateDetails.newResource == null) {
      operation = {
        type: 'delete',
        value: {
          id: refResource.id,
          ...viewData.affectingData
        }
      };
    } else {
      let newViewData = newViewDataMap[viewData.view];
      operation = {
        type: 'update',
        value: {
          id: refResource.id,
          ...newViewData.affectingData
        }
      };
    }
    this.notifyViewUpdate({
      type: viewData.type,
      view: viewData.view,
      params: viewData.params
    }, operation);
  });

  newResourceAffectedViews.forEach((viewData) => {
    if (!this._areObjectsEqual(oldViewParamsMap[viewData.view], viewData.params)) {
      let operation;
      if (updateDetails.oldResource == null) {
        operation = {
          type: 'create',
          value: {
            id: refResource.id,
            ...viewData.affectingData
          }
        };
      } else {
        operation = {
          type: 'update',
          value: {
            id: refResource.id,
            ...viewData.affectingData
          }
        };
      }
      this.notifyViewUpdate({
        type: viewData.type,
        view: viewData.view,
        params: viewData.params
      }, operation);
    }
  });
};

// Add a new document to a collection. This will send a change notification to each
// affected view (taking into account the affected page number within each view).
// This allows views to update themselves on the front-end in real-time.
AGCRUDRethink.prototype.create = async function (query, socket) {
  this._validateQuery(query);
  return this._create(query, socket);
};

AGCRUDRethink.prototype._create = async function (query, socket) {
  return new Promise((resolve, reject) => {
    let modelValidator = this.modelValidators[query.type];

    let savedHandler = (err, result) => {
      if (err) {
        this.emit('error', {error: err});
        this.emit('createFail', {query, error: err});
        reject(err);
      } else {
        let resourceChannelName = this._getResourceChannelName({
          type: query.type,
          id: result.id
        });
        this.publish(resourceChannelName);

        let affectedViewData = this.getQueryAffectedViews(query, result);
        affectedViewData.forEach((viewData) => {
          this.publish(this._getViewChannelName(viewData.view, viewData.params, viewData.type), {
            type: 'create',
            value: {
              id: result.id,
              ...viewData.affectingData
            }
          });
        });

        this.emit('create', {query, result});
        resolve(result.id);
      }
    };

    if (modelValidator == null) {
      let error = new Error('The ' + query.type + ' model type is not supported - It is not part of the schema');
      error.name = 'CRUDInvalidModelType';
      savedHandler(error);
    } else if (query.value && typeof query.value === 'object') {
      try {
        query.value = modelValidator(query.value);
      } catch (error) {
        savedHandler(error);
        return;
      }
      this.rethink.table(query.type)
        .insert(query.value, {returnChanges: true})
        .run()
        .then((result) => {
          if (result.errors) {
            savedHandler(errors.create(result.first_error));
            return;
          }
          savedHandler(null, result.changes[0].new_val);
        })
        .catch((err) => savedHandler(err));
    } else {
      let error = new Error('Cannot create a document from a primitive - Must be an object');
      error.name = 'CRUDInvalidParams';
      savedHandler(error);
    }
  });
};

AGCRUDRethink.prototype._appendToResourceReadBuffer = function (resourceChannelName, loadedHandler) {
  if (!this._resourceReadBuffer[resourceChannelName]) {
    this._resourceReadBuffer[resourceChannelName] = [];
  }
  this._resourceReadBuffer[resourceChannelName].push(loadedHandler);
};

AGCRUDRethink.prototype._processResourceReadBuffer = function (error, resourceChannelName, query, dataProvider) {
  let callbackList = this._resourceReadBuffer[resourceChannelName] || [];
  if (error) {
    callbackList.forEach((callback) => {
      callback(error);
    });
  } else {
    callbackList.forEach((callback) => {
      this.cache.pass(query, dataProvider, callback);
    });
  }
  delete this._resourceReadBuffer[resourceChannelName];
};

// Read either a collection of IDs, a single document or a single field
// within a document. To achieve efficient field-level granularity, a cache is used.
// A cache entry will automatically get cleared when ag-crud-rethink detects
// a real-time change to a field which is cached.
AGCRUDRethink.prototype.read = async function (query, socket) {
  this._validateQuery(query);
  return this._read(query, socket);
};

AGCRUDRethink.prototype._read = async function (query, socket) {
  return new Promise((resolve, reject) => {
    let pageSize = query.pageSize || this.options.defaultPageSize;

    let loadedHandler = async (err, data, count) => {
      if (err) {
        reject(err);
        return;
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

      try {
        await applyPostAccessFilter(accessFilterRequest);
      } catch (error) {
        reject(error);
        return;
      }
      let result;
      if (query.id) {
        if (query.field) {
          if (data == null) {
            data = {};
          }
          result = data[query.field];
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
        result = null;
      }

      resolve(result);
    };

    let modelValidator = this.modelValidators[query.type];
    if (modelValidator == null) {
      let error = new Error('The ' + query.type + ' model type is not supported - It is not part of the schema');
      error.name = 'CRUDInvalidModelType';
      loadedHandler(error);
    } else {
      if (query.id) {
        let dataProvider = (cb) => {
          this.rethink.table(query.type)
            .get(query.id)
            .run()
            .then((result) => {
              cb(null, result);
            })
            .catch((err) => cb(err));
        };
        let resourceChannelName = this._getResourceChannelName(query);

        let isSubscribedToResourceChannel = this.agServer.exchange.isSubscribed(resourceChannelName);
        let isSubscribedToResourceChannelOrPending = this.agServer.exchange.isSubscribed(resourceChannelName, true);
        let isSubcriptionPending = !isSubscribedToResourceChannel && isSubscribedToResourceChannelOrPending;

        this._appendToResourceReadBuffer(resourceChannelName, loadedHandler);

        if (isSubscribedToResourceChannel) {
          // If it is fully subscribed, we can process the request straight away since we are
          // confident that the data is up to date (in real-time).
          this._processResourceReadBuffer(null, resourceChannelName, query, dataProvider);
        } else if (!isSubcriptionPending) {
          // If there is no pending subscription, then we should create one and process the
          // buffer when we're subscribed.
          let handleResourceSubscribe = () => {
            resourceChannel.killListener('subscribeFail');
            this._processResourceReadBuffer(null, resourceChannelName, query, dataProvider);
          };
          let handleResourceSubscribeFailure = (err) => {
            resourceChannel.killListener('subscribe');
            let error = new Error('Failed to subscribe to resource channel for the ' + query.type + ' model');
            error.name = 'FailedToSubscribeToResourceChannel';
            this._processResourceReadBuffer(error, resourceChannelName, query, dataProvider);
          };

          let resourceChannel = this.agServer.exchange.subscribe(resourceChannelName);

          (async () => {
            for await (let data of resourceChannel) {
              this._handleResourceChange(query);
            }
          })();

          if (resourceChannel.state === 'subscribed') {
            this._processResourceReadBuffer(null, resourceChannelName, query, dataProvider);
          } else {
            (async () => {
              await resourceChannel.listener('subscribe').once();
              handleResourceSubscribe();
            })();
            (async () => {
              let {error} = await resourceChannel.listener('subscribeFail').once();
              handleResourceSubscribeFailure(error);
            })();
          }
        }
      } else {
        let rethinkQuery = constructTransformedRethinkQuery(this.options, this.rethink.table(query.type), query.type, query.view, query.viewParams);

        let tasks = [];

        if (query.offset) {
          tasks.push((cb) => {
            // Get one extra record just to check if we have the last value in the sequence.
            rethinkQuery.slice(query.offset, query.offset + pageSize + 1).pluck('id').run()
              .then((result) => {
                if (result.errors) {
                  cb(errors.create(result.first_error));
                  return;
                }
                cb(null, result);
              })
              .catch((err) => cb(err));
          });
        } else {
          tasks.push((cb) => {
            // Get one extra record just to check if we have the last value in the sequence.
            rethinkQuery.limit(pageSize + 1).pluck('id').run()
              .then((result) => {
                if (result.errors) {
                  cb(errors.create(result.first_error));
                  return;
                }
                cb(null, result);
              })
              .catch((err) => cb(err));
          });
        }

        if (query.getCount) {
          tasks.push((cb) => {
            rethinkQuery.count().run()
              .then((result) => {
                if (result.errors) {
                  cb(errors.create(result.first_error));
                  return;
                }
                cb(null, result);
              })
              .catch((err) => cb(err));
          });
        }

        async.parallel(tasks, (err, results) => {
          if (err) {
            let error = new Error(`Failed to generate view ${query.view} for type ${query.type} with viewParams ${JSON.stringify(query.viewParams)}`);
            // Emit both the low level and high level error.
            this.emit('error', {error: err});
            this.emit('error', {error});
            loadedHandler(error);
          } else {
            loadedHandler(null, results[0], results[1]);
          }
        });
      }
    }
  });
};

// Update a single whole document or one or more fields within a document.
// Whenever a document is updated, it may affect the ordering and pagination of
// certain views. This update operation will send notifications to all affected
// clients to let them know if a view that they are currently looking at
// has been affected by the update operation - This allows them to update
// themselves in real-time.
AGCRUDRethink.prototype.update = async function (query, socket) {
  this._validateQuery(query);
  return this._update(query, socket);
};

AGCRUDRethink.prototype._update = async function (query, socket) {
  return new Promise((resolve, reject) => {
    let savedHandler = (err, oldAffectedViewData, result) => {
      if (err) {
        this.emit('error', {error: err});
        this.emit('updateFail', {query, error: err});
        reject(err);
      } else {
        let resourceChannelName = this._getResourceChannelName(query);
        this.publish(resourceChannelName);

        if (query.field) {
          let cleanValue = query.value;
          if (cleanValue === undefined) {
            cleanValue = null;
          }
          if (typeof cleanValue === 'function') {
            // Do not publish raw RethinkDB predicates or functions.
            this.publish(this.channelPrefix + query.type + '/' + query.id + '/' + query.field);
          } else {
            this.publish(this.channelPrefix + query.type + '/' + query.id + '/' + query.field, {
              type: 'update',
              value: cleanValue
            });
          }
        } else {
          let queryValue = query.value || {};
          Object.keys(queryValue).forEach((field) => {
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
                value
              });
            }
          });
        }

        let oldViewDataMap = {};
        oldAffectedViewData.forEach((viewData) => {
          oldViewDataMap[`${viewData.view}:${viewData.type}`] = viewData;
        });

        let newAffectedViewData = this.getQueryAffectedViews(query, result);

        newAffectedViewData.forEach((viewData) => {
          let oldViewData = oldViewDataMap[`${viewData.view}:${viewData.type}`] || {};
          let areViewParamsEqual = this._areObjectsEqual(oldViewData.params, viewData.params);

          if (areViewParamsEqual) {
            let areAffectingDataEqual = this._areObjectsEqual(oldViewData.affectingData, viewData.affectingData);

            if (!areAffectingDataEqual) {
              this.publish(this._getViewChannelName(viewData.view, viewData.params, viewData.type), {
                type: 'update',
                value: {
                  id: query.id,
                  ...viewData.affectingData
                }
              });
            }
          } else {
            this.publish(this._getViewChannelName(oldViewData.view, oldViewData.params, oldViewData.type), {
              type: 'update',
              value: {
                id: query.id,
                ...viewData.affectingData
              }
            });
            this.publish(this._getViewChannelName(viewData.view, viewData.params, viewData.type), {
              type: 'update',
              value: {
                id: query.id,
                ...viewData.affectingData
              }
            });
          }
        });
        this.emit('update', {query, result});
        resolve();
      }
    };

    let modelValidator = this.modelValidators[query.type];
    if (modelValidator == null) {
      let error = new Error('The ' + query.type + ' model type is not supported - It is not part of the schema');
      error.name = 'CRUDInvalidModelType';
      savedHandler(error);
    } else if (query.id == null) {
      let error = new Error('Cannot update document without specifying an id');
      error.name = 'CRUDInvalidParams';
      savedHandler(error);
    } else {
      let tasks = [];

      // If socket does not exist, then the CRUD operation comes from the server-side
      // and we don't need to pass it through a accessFilter.
      let applyPostAccessFilter;
      if (socket && this.accessFilter) {
        applyPostAccessFilter = this.accessFilter.applyPostAccessFilter.bind(this.accessFilter);
      } else {
        applyPostAccessFilter = () => Promise.resolve();
      }

      let accessFilterRequest = {
        r: this.rethink,
        socket,
        action: 'update',
        authToken: socket && socket.authToken,
        query
      };

      let modelInstance;
      let loadModelInstanceAndGetViewData = (cb) => {
        this.rethink.table(query.type)
          .get(query.id)
          .run()
          .then((result) => {
            modelInstance = result;
            let oldAffectedViewData = this.getQueryAffectedViews(query, modelInstance);
            cb(null, oldAffectedViewData);
          })
          .catch((err) => cb(err));
      };

      if (query.field) {
        if (query.field === 'id') {
          let error = new Error('Cannot modify the id field of an existing document');
          error.name = 'CRUDInvalidOperation';
          savedHandler(error);
        } else {
          tasks.push(loadModelInstanceAndGetViewData);

          tasks.push(async (cb) => {
            accessFilterRequest.resource = modelInstance;
            try {
              await applyPostAccessFilter(accessFilterRequest);
            } catch (error) {
              cb(error);
              return;
            }

            let queryValue;

            try {
              queryValue = modelValidator({[query.field]: query.value}, true);
            } catch (error) {
              cb(error);
              return;
            }

            this._updateDb(query.type, query.id, queryValue)
              .then((result) => {
                cb(null, result);
              })
              .catch((err) => cb(err));
          });
        }
      } else {
        if (query.value && typeof query.value === 'object') {
          tasks.push(loadModelInstanceAndGetViewData);

          tasks.push(async (cb) => {
            accessFilterRequest.resource = modelInstance;
            try {
              await applyPostAccessFilter(accessFilterRequest);
            } catch (error) {
              cb(error);
              return;
            }
            try {
              query.value = modelValidator(query.value, true);
            } catch (error) {
              cb(error);
              return;
            }
            this._updateDb(query.type, query.id, query.value)
              .then((result) => {
                cb(null, result);
              })
              .catch((err) => cb(err));
          });
        } else {
          let error = new Error('Cannot replace document with a primitive - Must be an object');
          error.name = 'CRUDInvalidOperation';
          savedHandler(error);
        }
      }
      if (tasks.length) {
        async.series(tasks, (err, results) => {
          if (err) {
            savedHandler(err);
          } else {
            savedHandler(null, results[0], results[1]);
          }
        });
      }
    }
  });
};

// Delete a single document or field from a document.
// This will notify affected views so that they may update themselves
// in real-time.
AGCRUDRethink.prototype.delete = async function (query, socket) {
  this._validateQuery(query);
  return this._delete(query, socket);
};

AGCRUDRethink.prototype._delete = async function (query, socket) {
  return new Promise((resolve, reject) => {
    let deletedHandler = (err, oldAffectedViewData, result) => {
      if (err) {
        this.emit('error', {error: err});
        this.emit('deleteFail', {query, error: err});
        reject(err);
      } else {
        let resourceChannelName = this._getResourceChannelName(query);
        this.publish(resourceChannelName);

        if (query.field) {
          this.publish(this.channelPrefix + query.type + '/' + query.id + '/' + query.field, {
            type: 'delete'
          });
        } else {
          let deletedFields;
          let modelSchema = this.schema[query.type];
          if (modelSchema && modelSchema.fields) {
            deletedFields = modelSchema.fields;
          } else {
            deletedFields = result;
          }

          oldAffectedViewData.forEach((viewData) => {
            this.publish(this._getViewChannelName(viewData.view, viewData.params, viewData.type), {
              type: 'delete',
              value: {
                id: query.id,
                ...viewData.affectingData
              }
            });
          });
          Object.keys(deletedFields || {}).forEach((field) => {
            this.publish(this.channelPrefix + query.type + '/' + query.id + '/' + field, {
              type: 'delete'
            });
          });
        }
        this.emit('delete', {query, result});
        resolve();
      }
    };

    let modelValidator = this.modelValidators[query.type];
    if (modelValidator == null) {
      let error = new Error('The ' + query.type + ' model type is not supported - It is not part of the schema');
      error.name = 'CRUDInvalidModelType';
      deletedHandler(error);
    } else {
      let tasks = [];

      if (query.id == null) {
        let error = new Error('Cannot delete an entire collection - ID must be provided');
        error.name = 'CRUDInvalidParams';
        deletedHandler(error);
      } else {
        let modelInstance;
        tasks.push((cb) => {
          this.rethink.table(query.type)
            .get(query.id)
            .run()
            .then((result) => {
              modelInstance = result;
              let oldAffectedViewData = this.getQueryAffectedViews(query, modelInstance);
              cb(null, oldAffectedViewData);
            })
            .catch((err) => cb(err));
        });

        // If socket does not exist, then the CRUD operation comes from the server-side
        // and we don't need to pass it through a accessFilter.
        let applyPostAccessFilter;
        if (socket && this.accessFilter) {
          applyPostAccessFilter = this.accessFilter.applyPostAccessFilter.bind(this.accessFilter);
        } else {
          applyPostAccessFilter = () => Promise.resolve();
        }

        let accessFilterRequest = {
          r: this.rethink,
          socket,
          action: 'delete',
          authToken: socket && socket.authToken,
          query
        };

        if (query.field == null) {
          tasks.push(async (cb) => {
            accessFilterRequest.resource = modelInstance;
            try {
              await applyPostAccessFilter(accessFilterRequest);
            } catch (error) {
              cb(error);
              return;
            }
            this.rethink.table(query.type).get(query.id).delete().run()
              .then((result) => {
                if (result.errors) {
                  cb(errors.create(result.first_error));
                  return;
                }
                cb(null, result);
              })
              .catch((err) => cb(err));
          });
        } else {
          tasks.push(async (cb) => {
            accessFilterRequest.resource = modelInstance;
            try {
              await applyPostAccessFilter(accessFilterRequest);
            } catch (error) {
              cb(error);
              return;
            }
            try {
              modelValidator({[query.field]: undefined}, true);
            } catch (error) {
              cb(error);
              return;
            }
            this.rethink.table(query.type).get(query.id)
              .replace(
                (row) => {
                  return row.without(query.field);
                },
                {returnChanges: true}
              )
              .run()
              .then((result) => {
                if (result.errors) {
                  cb(errors.create(result.first_error));
                  return;
                }
                cb(null, result.changes.length ? result.changes[0].new_val : {});
              })
              .catch((err) => cb(err));
          });
        }
        if (tasks.length) {
          async.series(tasks, (err, results) => {
            if (err) {
              deletedHandler(err);
            } else {
              deletedHandler(null, results[0], results[1]);
            }
          });
        }
      }
    }
  });
};

AGCRUDRethink.prototype._attachSocket = function (socket) {
  (async () => {
    for await (let request of socket.procedure('create')) {
      let result;
      try {
        result = await this._create(request.data, socket);
      } catch (error) {
        request.error(
          this.clientErrorMapper(error, 'create', request.data)
        );
        continue;
      }
      request.end(result);
    }
  })();
  (async () => {
    for await (let request of socket.procedure('read')) {
      let result;
      try {
        result = await this._read(request.data, socket);
      } catch (error) {
        request.error(
          this.clientErrorMapper(error, 'read', request.data)
        );
        continue;
      }
      request.end(result);
    }
  })();
  (async () => {
    for await (let request of socket.procedure('update')) {
      try {
        await this._update(request.data, socket);
      } catch (error) {
        request.error(
          this.clientErrorMapper(error, 'update', request.data)
        );
        continue;
      }
      request.end();
    }
  })();
  (async () => {
    for await (let request of socket.procedure('delete')) {
      try {
        await this._delete(request.data, socket);
      } catch (error) {
        request.error(
          this.clientErrorMapper(error, 'delete', request.data)
        );
        continue;
      }
      request.end();
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
