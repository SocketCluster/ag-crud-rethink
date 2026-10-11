const { constructTransformedRethinkQuery } = require('./query-transformer');
const { parseChannelResourceQuery } = require('./channel-resource-parser');
const AsyncStreamEmitter = require('async-stream-emitter');
const jsonStableStringify = require('json-stable-stringify');
const { validateQuery, getOwnProperty } = require('./validate');

let AccessController = function (agServer, options) {
  AsyncStreamEmitter.call(this);

  this.options = options || {};
  this.schema = this.options.schema || {};
  this.maxPageSize = this.options.maxPageSize || null;
  this.rethink = this.options.rethink;
  this.cache = this.options.cache;
  this.agServer = agServer;

  this._getModelAccessFilter = (modelType, accessPhase) => {
    if (modelType == null || typeof modelType !== 'string') {
      return null;
    }
    let modelSchema = getOwnProperty(this.schema, modelType);
    if (!modelSchema) {
      return null;
    }
    let modelAccessFilters = modelSchema.access;
    if (!modelAccessFilters) {
      return null;
    }
    return modelAccessFilters[accessPhase] || null;
  };

  this._getComputedModelSchema = (type) => {
    return {
      maxPageSize: this.maxPageSize,
      ...getOwnProperty(this.schema, type)
    };
  };

  let middleware = this.options.middleware || {};

  agServer.setMiddleware(agServer.MIDDLEWARE_INBOUND, async (middlewareStream) => {
    for await (let action of middlewareStream) {
      let middlewareFunction = middleware[action.type];
      if (middlewareFunction) {
        let {type, allow, block, ...simpleAction} = action;
        try {
          await middlewareFunction(simpleAction);
        } catch (error) {
          action.block(error);
          continue;
        }
      }

      if (action.type === action.INVOKE) {
        if (action.procedure === 'crud') {
          let query = action.data;
          try {
            validateQuery(query, this.schema);
          } catch (validationError) {
            let error = new Error(
              `Query failed validation because of error: ${validationError.message}`
            );
            error.name = 'CRUDBlockedError';
            error.type = 'pre';
            action.block(error);
            continue;
          }

          let isCollectionRead = query.action === 'read' && !query.id;

          if (isCollectionRead && !query.view && this.options.requireViewForCollectionRead) {
            let error = new Error(
              `You are not permitted to read the ${query.type} collection without specifying a view`
            );
            error.name = 'CRUDBlockedError';
            error.type = 'pre';
            action.block(error);
            continue;
          }

          // The maxPageSize applies to every collection read, with or without a view.
          if (isCollectionRead && typeof query.pageSize === 'number') {
            let {maxPageSize} = this._getComputedModelSchema(query.type);
            if (maxPageSize != null && query.pageSize > maxPageSize) {
              let target = query.view ? `the ${query.view} view of the ${query.type} model` : `the ${query.type} collection`;
              let error = new Error(
                `You are not permitted to access ${target} - Query pageSize exceeded the maxPageSize of ${maxPageSize}`
              );
              error.name = 'CRUDBlockedError';
              error.type = 'pre';
              action.block(error);
              continue;
            }
          }

          // If socket has a valid auth token, then allow emitting get or set events
          let authToken = action.socket.authToken;

          let preAccessFilter = this._getModelAccessFilter(query.type, 'pre');
          if (preAccessFilter) {
            let crudRequest = {
              r: this.rethink,
              socket: action.socket,
              action: query.action,
              authToken,
              query
            };
            try {
              await preAccessFilter(crudRequest);
            } catch (error) {
              if (typeof error === 'boolean') {
                error = new Error(
                  `You are not permitted to perform a CRUD operation on the ${query.type} resource with ID ${query.id}`
                );
                error.name = 'CRUDBlockedError';
                error.type = 'pre';
              }
              action.block(error);
              continue;
            }
            action.allow();
            continue;
          }
          if (this.options.blockPreByDefault) {
            let crudBlockedError = new Error(
              `You are not permitted to perform a CRUD operation on the ${query.type} resource with ID ${query.id} - No access filters found`
            );
            crudBlockedError.name = 'CRUDBlockedError';
            crudBlockedError.type = 'pre';
            action.block(crudBlockedError);
            continue;
          }
          action.allow();
          continue;
        }
        action.allow();
        continue;
      }

      if (action.type === action.PUBLISH_IN) {
        let channelResourceQuery = parseChannelResourceQuery(action.channel);
        if (channelResourceQuery) {
          // Always block CRUD publish from outside clients.
          let crudPublishNotAllowedError = new Error('Cannot publish to a CRUD resource channel');
          crudPublishNotAllowedError.name = 'CRUDPublishNotAllowedError';
          action.block(crudPublishNotAllowedError);
          continue;
        }
        action.allow();
        continue;
      }

      if (action.type === action.SUBSCRIBE) {
        let authToken = action.socket.authToken;
        let channelResourceQuery = parseChannelResourceQuery(action.channel);
        if (!channelResourceQuery) {
          action.allow();
          continue;
        }
        channelResourceQuery.action = action.SUBSCRIBE;

        try {
          // Sometimes the real viewParams may be different from what can be parsed from
          // the channel name; this is because some view params don't affect the real-time
          // delivery of messages but they may still be useful in constructing the view.
          // Params encoded in the channel name always take precedence, so access checks
          // are evaluated against the channel the socket is actually subscribing to.
          if (channelResourceQuery.view != null && action.data && action.data.viewParams && typeof action.data.viewParams === 'object') {
            channelResourceQuery.viewParams = this._mergeSubscribeViewParams(
              channelResourceQuery,
              action.data.viewParams
            );
          }
          validateQuery(channelResourceQuery, this.schema);
        } catch (validationError) {
          let error = new Error(
            `Subscribe query failed validation because of error: ${validationError.message}`
          );
          error.name = 'CRUDBlockedError';
          error.type = 'pre';
          action.block(error);
          continue;
        }

        let continueWithPostAccessFilter = async () => {
          let subscribePostRequest = {
            socket: action.socket,
            action: 'subscribe',
            query: channelResourceQuery,
            fetchResource: true
          };
          let result;
          try {
            result = await this.applyPostAccessFilter(subscribePostRequest);
          } catch (error) {
            action.block(error);
            return;
          }
          action.allow(result);
        };

        let preAccessFilter = this._getModelAccessFilter(channelResourceQuery.type, 'pre');
        if (preAccessFilter) {
          let subscribePreRequest = {
            r: this.rethink,
            socket: action.socket,
            action: 'subscribe',
            authToken,
            query: channelResourceQuery
          };
          try {
            await preAccessFilter(subscribePreRequest);
          } catch (error) {
            if (typeof error === 'boolean') {
              error = new Error(`Cannot subscribe to the ${action.channel} channel`);
              error.name = 'CRUDBlockedError';
              error.type = 'pre';
            }
            action.block(error);
            continue;
          }
          await continueWithPostAccessFilter();
          continue;
        }
        if (this.options.blockPreByDefault) {
          let crudBlockedError = new Error(
            `Cannot subscribe to the ${action.channel} channel - No access filters found`
          );
          crudBlockedError.name = 'CRUDBlockedError';
          crudBlockedError.type = 'pre';
          action.block(crudBlockedError);
          continue;
        }
        await continueWithPostAccessFilter();
        continue;
      }

      action.allow();
      continue;
    }
  });

  agServer.setMiddleware(agServer.MIDDLEWARE_OUTBOUND, async (middlewareStream) => {
    for await (let action of middlewareStream) {
      let middlewareFunction = middleware[action.type];
      if (middlewareFunction) {
        let {type, allow, block, ...simpleAction} = action;
        try {
          await middlewareFunction(simpleAction);
        } catch (error) {
          action.block(error);
          continue;
        }
      }

      if (action.type === action.PUBLISH_OUT) {
        let actionData = action.data;
        if (actionData && typeof actionData === 'object') {
          let {publisherSocketId, publisherId, ...payload} = actionData;
          if (publisherSocketId === action.socket.id) {
            if (publisherId) {
              payload.publisherId = publisherId;
              action.allow({data: payload});
              continue;
            }
            // Block silently.
            action.block(false);
            continue;
          }
          action.allow({data: payload});
          continue;
        }
      }
      action.allow();
    }
  });
};

AccessController.prototype = Object.create(AsyncStreamEmitter.prototype);

// Compares a param value parsed from a view channel name with one supplied by the client.
// Unless typedViewChannelParams is enabled, channel names carry stringified values.
AccessController.prototype._areViewParamValuesEqual = function (channelValue, value) {
  if (this.options.typedViewChannelParams) {
    return jsonStableStringify(channelValue ?? null) === jsonStableStringify(value ?? null);
  }
  return String(channelValue ?? null) === String(value ?? null);
};

// Merges viewParams supplied in subscription data with the params encoded in the
// view channel name. Throws if they conflict, or if the client supplies params which
// should have been part of the channel name.
AccessController.prototype._mergeSubscribeViewParams = function (channelResourceQuery, dataViewParams) {
  let channelViewParams = channelResourceQuery.viewParams;
  if (channelViewParams == null || typeof channelViewParams !== 'object' || Array.isArray(channelViewParams)) {
    throw new Error('The view channel name did not contain valid viewParams');
  }
  let modelSchema = getOwnProperty(this.schema, channelResourceQuery.type);
  let viewSchema = getOwnProperty(modelSchema && modelSchema.views, channelResourceQuery.view);
  let hasPrimaryFields = !!(viewSchema && viewSchema.primaryFields && viewSchema.primaryFields.length);

  let mergedViewParams = {...dataViewParams};
  for (let [field, channelValue] of Object.entries(channelViewParams)) {
    if (Object.hasOwn(dataViewParams, field)) {
      if (!this._areViewParamValuesEqual(channelValue, dataViewParams[field])) {
        throw new Error(`The ${field} viewParam did not match the value in the channel name`);
      }
    } else {
      mergedViewParams[field] = channelValue;
    }
  }
  // Without primaryFields, every view param is encoded in the channel name.
  if (!hasPrimaryFields) {
    for (let field of Object.keys(dataViewParams)) {
      if (!Object.hasOwn(channelViewParams, field)) {
        throw new Error(`The ${field} viewParam was not part of the channel name`);
      }
    }
  }
  return mergedViewParams;
};

AccessController.prototype.applyPreAccessFilter = async function (req) {
  let authToken = req.socket?.authToken;
  let {query} = req;
  let preAccessFilter = this._getModelAccessFilter(query.type, 'pre');
  if (preAccessFilter) {
    let crudRequest = {
      r: this.rethink,
      socket: req.socket,
      action: query.action,
      authToken,
      query
    };
    await preAccessFilter(crudRequest);
  }
};

AccessController.prototype.applyPostAccessFilter = async function (req) {
  let {query} = req;
  let postAccessFilter = this._getModelAccessFilter(query.type, 'post');

  if (postAccessFilter) {
    let request = {
      r: this.rethink,
      socket: req.socket,
      action: req.action,
      authToken: req.socket && req.socket.authToken,
      query
    };

    if (req.fetchResource) {
      let pageSize = query.pageSize ?? this.options.defaultPageSize;

      if (!getOwnProperty(this.schema, query.type)) {
        let error = new Error(`The ${query.type} model type is not supported - It is not part of the schema`);
        error.name = 'CRUDInvalidModelType';
        this.emit('error', {error});
        throw error;
      }

      if (query.id) {
        try {
          request.resource = await this.cache.pass(query, async () => {
            return await this.rethink.table(query.type).get(query.id).run();
          });
        } catch (error) {
          this.emit('error', {error});
          throw new Error('Failed to preload resource due to an unexpected error');
        }
      } else {
        // For collections.
        try {
          let rethinkQuery = constructTransformedRethinkQuery(this.options, this.rethink.table(query.type), query.type, query.view, query.viewParams);
          if (query.offset) {
            rethinkQuery = rethinkQuery.slice(query.offset, query.offset + pageSize).pluck('id');
          } else {
            rethinkQuery = rethinkQuery.limit(pageSize).pluck('id');
          }
          request.resource = await rethinkQuery.run();
        } catch (error) {
          this.emit('error', {error});
          throw new Error('Executed an invalid query transformation');
        }
      }
    } else {
      request.resource = req.resource;
    }

    try {
      await postAccessFilter(request);
    } catch (error) {
      if (typeof error === 'boolean') {
        error = new Error(`You are not permitted to perform a CRUD operation on the ${query.type} resource with ID ${query.id}`);
        error.name = 'CRUDBlockedError';
        error.type = 'post';
      }
      this.emit('error', {error});
      throw error;
    }
  } else {
    if (this.options.blockPostByDefault) {
      let crudBlockedError = new Error(`You are not permitted to perform a CRUD operation on the ${query.type} resource with ID ${query.id} - No access filters found`);
      crudBlockedError.name = 'CRUDBlockedError';
      crudBlockedError.type = 'post';
      this.emit('error', {error: crudBlockedError});
      throw crudBlockedError;
    }
  }
};

module.exports = AccessController;
