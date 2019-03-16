const thinky = require('thinky');
const async = require('async');
const Filter = require('./filter');
const Cache = require('./cache');
const AsyncStreamEmitter = require('async-stream-emitter');
const jsonStableStringify = require('json-stable-stringify');
const constructTransformedRethinkQuery = require('./query-transformer').constructTransformedRethinkQuery;
const parseChannelResourceQuery = require('./channel-resource-parser').parseChannelResourceQuery;

let AGCRUDRethink = function (options) {
  AsyncStreamEmitter.call(this);

  this.options = Object.assign({}, options);

  if (!this.options.logger) {
    this.options.logger = console;
  }
  if (!this.options.schema) {
    this.options.schema = {};
  }

  this.models = {};
  this.schema = this.options.schema;
  this.thinky = thinky(this.options.thinkyOptions);
  this.options.thinky = this.thinky;
  this.logger = this.options.logger;

  this.channelPrefix = 'crud>';

  if (!this.options.defaultPageSize) {
    this.options.defaultPageSize = 10;
  }

  Object.keys(this.schema).forEach((modelName) => {
    let modelSchema = this.schema[modelName];
    this.models[modelName] = this.thinky.createModel(modelName, modelSchema.fields);
  });
  this.options.models = this.models;

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
    this.filter = new Filter(this.agServer, this.options);

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

  Object.keys(viewSchemaMap).forEach((viewName) => {
    let viewSchema = viewSchemaMap[viewName];
    let paramFields = viewSchema.paramFields || [];
    let affectingFields = viewSchema.affectingFields || [];

    let params = {};
    let affectingData = {};
    paramFields.forEach((fieldName) => {
      params[fieldName] = resource[fieldName];
      affectingData[fieldName] = resource[fieldName];
    });
    affectingFields.forEach((fieldName) => {
      affectingData[fieldName] = resource[fieldName];
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
          type: updateDetails.type,
          params,
          affectingData
        });
      }
    } else {
      affectedViews.push({
        view: viewName,
        type: updateDetails.type,
        params,
        affectingData
      });
    }
  });
  return affectedViews;
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
      this.publish(resourcePropertyChannelName, {
        type: 'update',
        value: updatedFields[fieldName]
      });
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
AGCRUDRethink.prototype.notifyViewUpdate = function (updateDetails) {
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
  this.publish(viewChannelName);
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

  oldResourceAffectedViews.forEach((viewData) => {
    oldViewParamsMap[viewData.view] = viewData.params;
    this.notifyViewUpdate({
      type: viewData.type,
      view: viewData.view,
      params: viewData.params
    });
  });

  let newResourceAffectedViews = this.getAffectedViews({
    type: updateDetails.type,
    resource: newResource,
    fields: updatedFieldsList
  });

  newResourceAffectedViews.forEach((viewData) => {
    if (!this._areObjectsEqual(oldViewParamsMap[viewData.view], viewData.params)) {
      this.notifyViewUpdate({
        type: viewData.type,
        view: viewData.view,
        params: viewData.params
      });
    }
  });
};

// Add a new document to a collection. This will send a change notification to each
// affected view (taking into account the affected page number within each view).
// This allows views to update themselves on the front-end in real-time.
AGCRUDRethink.prototype.create = async function (query, socket) {
  return new Promise((resolve, reject) => {
    this._create(query, (err, result) => {
      if (err) {
        reject(err);
        return;
      }
      resolve(result);
    }, socket);
  });
};

AGCRUDRethink.prototype._create = function (query, callback, socket) {
  let validationError = this._validateQuery(query);
  if (validationError) {
    callback && callback(validationError);
    return;
  }

  let ModelClass = this.models[query.type];

  let savedHandler = (err, result) => {
    if (err) {
      callback && callback(err);
    } else {
      let resourceChannelName = this._getResourceChannelName({
        type: query.type,
        id: result.id
      });
      this.publish(resourceChannelName);

      let affectedViewData = this.getQueryAffectedViews(query, result);
      affectedViewData.forEach((viewData) => {
        this.publish(this._getViewChannelName(viewData.view, viewData.params, query.type), {
          type: 'create',
          id: result.id
        });
      });

      callback && callback(null, result.id);
    }
  };

  if (ModelClass == null) {
    let error = new Error('The ' + query.type + ' model type is not supported - It is not part of the schema');
    error.name = 'CRUDInvalidModelType';
    savedHandler(error);
    return Promise.reject();
  } if (typeof query.value === 'object') {
    let instance = new ModelClass(query.value);
    instance.save(savedHandler);
  } else {
    let error = new Error('Cannot create a document from a primitive - Must be an object');
    error.name = 'CRUDInvalidParams';
    savedHandler(error);
  }
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
  return new Promise((resolve, reject) => {
    this._read(query, (err, result) => {
      if (err) {
        reject(err);
        return;
      }
      resolve(result);
    }, socket);
  });
};

AGCRUDRethink.prototype._read = function (query, callback, socket) {
  let validationError = this._validateQuery(query);
  if (validationError) {
    callback && callback(validationError);
    return;
  }

  let pageSize = query.pageSize || this.options.defaultPageSize;

  let loadedHandler = async (err, data, count) => {
    if (err) {
      callback && callback(err);
      return;
    }
    // If socket does not exist, then the CRUD operation comes from the server-side
    // and we don't need to pass it through a filter.
    let applyPostFilter;
    if (socket && this.filter) {
      applyPostFilter = this.filter.applyPostFilter.bind(this.filter);
    } else {
      applyPostFilter = () => Promise.resolve();
    }
    let filterRequest = {
      r: this.thinky.r,
      socket,
      action: 'read',
      authToken: socket && socket.authToken,
      query,
      resource: data
    };

    try {
      await applyPostFilter(filterRequest);
    } catch (error) {
      callback && callback(error);
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

    callback && callback(null, result);
  };

  let ModelClass = this.models[query.type];
  if (ModelClass == null) {
    let error = new Error('The ' + query.type + ' model type is not supported - It is not part of the schema');
    error.name = 'CRUDInvalidModelType';
    loadedHandler(error);
  } else {
    if (query.id) {
      let dataProvider = (cb) => {
        ModelClass.get(query.id).run((err, data) => {
          let error;
          if (err) {
            this.logger.error(err);
            error = new Error(`Failed to get resource with id ${query.id} from the database`);
          } else {
            error = null;
          }
          cb(error, data);
        });
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
      let rethinkQuery = constructTransformedRethinkQuery(this.options, ModelClass, query.type, query.view, query.viewParams);

      let tasks = [];

      if (query.offset) {
        tasks.push((cb) => {
          // Get one extra record just to check if we have the last value in the sequence.
          rethinkQuery.slice(query.offset, query.offset + pageSize + 1).pluck('id').run(cb);
        });
      } else {
        tasks.push((cb) => {
          // Get one extra record just to check if we have the last value in the sequence.
          rethinkQuery.limit(pageSize + 1).pluck('id').run(cb);
        });
      }

      if (query.getCount) {
        tasks.push((cb) => {
          rethinkQuery.count().execute(cb);
        });
      }

      async.parallel(tasks, (err, results) => {
        if (err) {
          let error = new Error(`Failed to generate view ${query.view} for type ${query.type} with viewParams ${JSON.stringify(query.viewParams)}`);
          this.logger.error(err);
          this.logger.error(error);
          loadedHandler(error);
        } else {
          loadedHandler(null, results[0], results[1]);
        }
      });
    }
  }
};

// Update a single whole document or one or more fields within a document.
// Whenever a document is updated, it may affect the ordering and pagination of
// certain views. This update operation will send notifications to all affected
// clients to let them know if a view that they are currently looking at
// has been affected by the update operation - This allows them to update
// themselves in real-time.
AGCRUDRethink.prototype.update = async function (query, socket) {
  return new Promise((resolve, reject) => {
    this._update(query, (err, result) => {
      if (err) {
        reject(err);
        return;
      }
      resolve(result);
    }, socket);
  });
};

AGCRUDRethink.prototype._update = function (query, callback, socket) {
  let validationError = this._validateQuery(query);
  if (validationError) {
    callback && callback(validationError);
    return;
  }

  let savedHandler = (err, oldAffectedViewData, result) => {
    if (err) {
      this.emit('warning', {warning: err});
      callback && callback(err);
    } else {
      let resourceChannelName = this._getResourceChannelName(query);
      this.publish(resourceChannelName);

      if (query.field) {
        let cleanValue = query.value;
        if (cleanValue === undefined) {
          cleanValue = null;
        }
        this.publish(this.channelPrefix + query.type + '/' + query.id + '/' + query.field, {
          type: 'update',
          value: cleanValue
        });
      } else {
        let queryValue = query.value || {};
        Object.keys(queryValue).forEach((field) => {
          let value = queryValue[field];
          if (value === undefined) {
            value = null;
          }
          this.publish(this.channelPrefix + query.type + '/' + query.id + '/' + field, {
            type: 'update',
            value
          });
        });
      }

      let oldViewDataMap = {};
      oldAffectedViewData.forEach((viewData) => {
        oldViewDataMap[viewData.view] = viewData;
      });

      let newAffectedViewData = this.getQueryAffectedViews(query, result);

      newAffectedViewData.forEach((viewData) => {
        let oldViewData = oldViewDataMap[viewData.view] || {};
        let areViewParamsEqual = this._areObjectsEqual(oldViewData.params, viewData.params);

        if (areViewParamsEqual) {
          let areAffectingDataEqual = this._areObjectsEqual(oldViewData.affectingData, viewData.affectingData);

          if (!areAffectingDataEqual) {
            this.publish(this._getViewChannelName(viewData.view, viewData.params, query.type), {
              type: 'update',
              action: 'move',
              id: query.id
            });
          }
        } else {
          this.publish(this._getViewChannelName(viewData.view, oldViewData.params, query.type), {
            type: 'update',
            action: 'remove',
            id: query.id
          });
          this.publish(this._getViewChannelName(viewData.view, viewData.params, query.type), {
            type: 'update',
            action: 'add',
            id: query.id
          });
        }
      });
      callback && callback();
    }
  };

  let ModelClass = this.models[query.type];
  if (ModelClass == null) {
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
    // and we don't need to pass it through a filter.
    let applyPostFilter;
    if (socket && this.filter) {
      applyPostFilter = this.filter.applyPostFilter.bind(this.filter);
    } else {
      applyPostFilter = () => Promise.resolve();
    }

    let filterRequest = {
      r: this.thinky.r,
      socket,
      action: 'update',
      authToken: socket && socket.authToken,
      query
    };

    let modelInstance;
    let loadModelInstanceAndGetViewData = (cb) => {
      ModelClass.get(query.id).run().then((instance) => {
        modelInstance = instance;
        let oldAffectedViewData = this.getQueryAffectedViews(query, modelInstance);
        cb(null, oldAffectedViewData);
      }).error(cb);
    };

    if (query.field) {
      if (query.field === 'id') {
        let error = new Error('Cannot modify the id field of an existing document');
        error.name = 'CRUDInvalidOperation';
        savedHandler(error);
      } else {
        tasks.push(loadModelInstanceAndGetViewData);

        tasks.push(async (cb) => {
          filterRequest.resource = modelInstance;
          try {
            await applyPostFilter(filterRequest);
          } catch (error) {
            cb(error);
            return;
          }
          modelInstance[query.field] = query.value;
          modelInstance.save(cb);
        });
      }
    } else {
      if (typeof query.value === 'object') {
        tasks.push(loadModelInstanceAndGetViewData);

        tasks.push(async (cb) => {
          filterRequest.resource = modelInstance;
          try {
            await applyPostFilter(filterRequest);
          } catch (error) {
            cb(error);
            return;
          }
          let queryValue = query.value || {};
          Object.keys(queryValue).forEach((field) => {
            modelInstance[field] = queryValue[field];
          });
          modelInstance.save(cb);
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
};

// Delete a single document or field from a document.
// This will notify affected views so that they may update themselves
// in real-time.
AGCRUDRethink.prototype.delete = async function (query, socket) {
  return new Promise((resolve, reject) => {
    this._delete(query, (err, result) => {
      if (err) {
        reject(err);
        return;
      }
      resolve(result);
    }, socket);
  });
};

AGCRUDRethink.prototype._delete = function (query, callback, socket) {
  let validationError = this._validateQuery(query);
  if (validationError) {
    callback && callback(validationError);
    return;
  }

  let deletedHandler = (err, oldAffectedViewData, result) => {
    if (err) {
      this.emit('warning', {warning: err});
      callback && callback(err);
    } else {
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
        Object.keys(deletedFields || {}).forEach((field) => {
          this.publish(this.channelPrefix + query.type + '/' + query.id + '/' + field, {
            type: 'delete'
          });
        });

        oldAffectedViewData.forEach((viewData) => {
          this.publish(this._getViewChannelName(viewData.view, viewData.params, query.type), {
            type: 'delete',
            id: query.id
          });
        });
      }
      callback && callback();
    }
  };

  let ModelClass = this.models[query.type];
  if (ModelClass == null) {
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
        ModelClass.get(query.id).run().then((instance) => {
          modelInstance = instance;
          let oldAffectedViewData = this.getQueryAffectedViews(query, modelInstance);
          cb(null, oldAffectedViewData);
        }).error(cb);
      });

      // If socket does not exist, then the CRUD operation comes from the server-side
      // and we don't need to pass it through a filter.
      let applyPostFilter;
      if (socket && this.filter) {
        applyPostFilter = this.filter.applyPostFilter.bind(this.filter);
      } else {
        applyPostFilter = () => Promise.resolve();
      }

      let filterRequest = {
        r: this.thinky.r,
        socket,
        action: 'delete',
        authToken: socket && socket.authToken,
        query
      };

      if (query.field == null) {
        tasks.push(async (cb) => {
          filterRequest.resource = modelInstance;
          try {
            await applyPostFilter(filterRequest);
          } catch (error) {
            cb(error);
            return;
          }
          modelInstance.delete(cb);
        });
      } else {
        tasks.push(async (cb) => {
          filterRequest.resource = modelInstance;
          try {
            await applyPostFilter(filterRequest);
          } catch (error) {
            cb(error);
            return;
          }
          delete modelInstance[query.field];
          modelInstance.save(cb);
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
};

AGCRUDRethink.prototype._attachSocket = function (socket) {
  (async () => {
    for await (let request of socket.procedure('create')) {
      let result;
      try {
        result = await this.create(request.data, socket);
      } catch (error) {
        request.error(error);
        continue;
      }
      request.end(result);
    }
  })();
  (async () => {
    for await (let request of socket.procedure('read')) {
      let result;
      try {
        result = await this.read(request.data, socket);
      } catch (error) {
        request.error(error);
        continue;
      }
      request.end(result);
    }
  })();
  (async () => {
    for await (let request of socket.procedure('update')) {
      try {
        await this.update(request.data, socket);
      } catch (error) {
        request.error(error);
        continue;
      }
      request.end();
    }
  })();
  (async () => {
    for await (let request of socket.procedure('delete')) {
      try {
        await this.delete(request.data, socket);
      } catch (error) {
        request.error(error);
        continue;
      }
      request.end();
    }
  })();
};

AGCRUDRethink.prototype._validateRequiredViewParams = function (viewParams) {
  if (viewParams === undefined || viewParams === null) {
    return new Error(`Invalid view query - The view ${query.view} under the type ${query.type} expects viewParams but it was null or undefined`);
  }
  let viewParamsType = typeof viewParams;
  if (viewParamsType !== 'object') {
    return new Error(`Invalid view query - The view ${query.view} under the type ${query.type} expects viewParams to be an object instead of ${viewParamsType}`);
  }
  return null;
};

AGCRUDRethink.prototype._validateViewQuery = function (query) {
  let viewSchema = this._getView(query.type, query.view);
  if (!viewSchema) {
    return new Error(`Invalid view query - The view ${query.view} was not defined in the schema under the type ${query.type}`);
  }
  if (viewSchema.paramFields && viewSchema.paramFields.length > 0) {
    let viewParamsFormatError = this._validateRequiredViewParams(query.viewParams)
    if (viewParamsFormatError) {
      return viewParamsFormatError;
    }
    let missingFields = [];
    viewSchema.paramFields.forEach((field) => {
      if (query.viewParams[field] === undefined) {
        missingFields.push(field);
      }
    });
    if (missingFields.length > 0) {
      return new Error(`Invalid view query - The view ${query.view} under the type ${query.type} requires additional fields to meet paramFields requirements. Missing: ${missingFields.join(', ')}`);
    }
  }
  if (viewSchema.primaryKeys && viewSchema.primaryKeys.length > 0) {
    let viewParamsFormatError = this._validateRequiredViewParams(query.viewParams)
    if (viewParamsFormatError) {
      return viewParamsFormatError;
    }
    let missingFields = [];
    viewSchema.primaryKeys.forEach((field) => {
      if (query.viewParams[field] === undefined) {
        missingFields.push(field);
      }
    });
    if (missingFields.length > 0) {
      return new Error(`Invalid view query - The view ${query.view} under the type ${query.type} requires additional fields to meet primaryKeys requirements. Missing: ${missingFields.join(', ')}`);
    }
  }
  return null;
};

AGCRUDRethink.prototype._validateQuery = function (query) {
  if (query === undefined || query === null) {
    return new Error(`Invalid query - The query was null or undefined`);
  }
  let queryType = typeof query;
  if (queryType !== 'object') {
    return new Error(`Invalid query - The query must be an object instead of ${queryType}`);
  }
  if (query.type === undefined || query.type === null) {
    return new Error('Invalid query - The query type cannot be null or undefined');
  }
  if (!this.schema[query.type]) {
    return new Error(`Invalid query - The query type ${query.type} was not defined on the schema`);
  }
  let fieldIsSet = query.field !== undefined && query.field !== null;
  let idIsSet = query.id !== undefined && query.id !== null;
  if (fieldIsSet) {
    let fieldType = typeof query.field;
    if (fieldType !== 'string') {
      return new Error(`Invalid field query - The field property must be a string instead of ${fieldType}`);
    }
    if (!idIsSet) {
      return new Error(`Invalid field query - The query must have an id property`);
    }
  }
  if (idIsSet) {
    let idType = typeof query.id;
    if (idType !== 'string') {
      return new Error(`Invalid resource query - The resource id must be a string instead of ${idType}`);
    }
  }
  let viewIsSet = query.view !== undefined && query.view !== null;
  if (viewIsSet) {
    let viewQueryError = this._validateViewQuery(query);
    if (viewQueryError) {
      return viewQueryError;
    }
  }
  return null;
};

module.exports.thinky = thinky;
module.exports.AGCRUDRethink = AGCRUDRethink;

module.exports.attach = function (agServer, options) {
  if (options) {
    options.agServer = agServer;
  } else {
    options = {agServer};
  }
  return new AGCRUDRethink(options);
};
