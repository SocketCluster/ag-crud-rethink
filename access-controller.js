const constructTransformedRethinkQuery = require('./query-transformer').constructTransformedRethinkQuery;
const parseChannelResourceQuery = require('./channel-resource-parser').parseChannelResourceQuery;
const AsyncStreamEmitter = require('async-stream-emitter');

let AccessController = function (agServer, options) {
  AsyncStreamEmitter.call(this);

  this.options = options || {};
  this.schema = this.options.schema || {};
  this.thinky = this.options.thinky;
  this.cache = this.options.cache;
  this.agServer = agServer;

  this._getModelAccessFilter = (modelType, accessPhase) => {
    let modelSchema = this.schema[modelType];
    if (!modelSchema) {
      return null;
    }
    let modelAccessFilters = modelSchema.access;
    if (!modelAccessFilters) {
      return null;
    }
    return modelAccessFilters[accessPhase] || null;
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
        if (action.procedure === 'create' || action.procedure === 'read' || action.procedure === 'update' || action.procedure === 'delete') {
          // If socket has a valid auth token, then allow emitting get or set events
          let authToken = action.socket.authToken;

          let preAccessFilter = this._getModelAccessFilter(action.data.type, 'pre');
          if (preAccessFilter) {
            let crudRequest = {
              r: this.thinky.r,
              socket: action.socket,
              action: action.procedure,
              authToken,
              query: action.data
            };
            try {
              await preAccessFilter(crudRequest);
            } catch (error) {
              if (typeof error === 'boolean') {
                error = new Error('You are not permitted to perform a CRUD operation on the ' + action.data.type + ' resource with ID ' + action.data.id);
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
            let crudBlockedError = new Error('You are not permitted to perform a CRUD operation on the ' + action.data.type + ' resource with ID ' + action.data.id + ' - No access filters found');
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
        // Sometimes the real viewParams may be different from what can be parsed from
        // the channel name; this is because some view params don't affect the real-time
        // delivery of messages but they may still be useful in constructing the view.
        if (channelResourceQuery.view !== undefined && action.data && typeof action.data.viewParams === 'object') {
          channelResourceQuery.viewParams = action.data.viewParams;
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
            r: this.thinky.r,
            socket: action.socket,
            action: 'subscribe',
            authToken,
            query: channelResourceQuery
          };
          try {
            await preAccessFilter(subscribePreRequest);
          } catch (error) {
            if (typeof error === 'boolean') {
              error = new Error('Cannot subscribe to ' + action.channel + ' channel');
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
          let crudBlockedError = new Error('Cannot subscribe to ' + action.channel + ' channel - No access filters found');
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
};

AccessController.prototype = Object.create(AsyncStreamEmitter.prototype);

AccessController.prototype.applyPostAccessFilter = async function (req) {
  return new Promise((resolve, reject) => {
    this._applyPostAccessFilter(req, (error, result) => {
      if (error) {
        reject(error);
        return;
      }
      resolve(result);
    });
  });
};

AccessController.prototype._applyPostAccessFilter = function (req, next) {
  let query = req.query;
  let postAccessFilter = this._getModelAccessFilter(query.type, 'post');

  if (postAccessFilter) {
    let request = {
      r: this.thinky.r,
      socket: req.socket,
      action: req.action,
      authToken: req.socket && req.socket.authToken,
      query
    };
    if (!req.fetchResource) {
      request.resource = req.resource;
    }

    let continueWithPostAccessFilter = () => {
      (async () => {
        try {
          await postAccessFilter(request);
        } catch (error) {
          if (typeof error === 'boolean') {
            error = new Error('You are not permitted to perform a CRUD operation on the ' + query.type + ' resource with ID ' + query.id);
            error.name = 'CRUDBlockedError';
            error.type = 'post';
          }
          next(error);

          return;
        }
        next();
      })();
    };

    if (req.fetchResource) {
      let pageSize = query.pageSize || this.options.defaultPageSize;
      let ModelClass = this.options.models[query.type];

      if (!ModelClass) {
        let error = new Error('The ' + query.type + ' model type is not supported - It is not part of the schema');
        error.name = 'CRUDInvalidModelType';
        next(error);
        return;
      }

      let queryResponseHandler = (error, resource) => {
        if (error) {
          this.emit('error', {error});
          next(new Error('Executed an invalid query transformation'));
        } else {
          request.resource = resource;
          continueWithPostAccessFilter();
        }
      };

      if (query.id) {
        let dataProvider = (cb) => {
          ModelClass.get(query.id).run(cb);
        };
        this.cache.pass(query, dataProvider, queryResponseHandler);
      } else {
        // For collections.
        let rethinkQuery = constructTransformedRethinkQuery(this.options, ModelClass, query.type, query.view, query.viewParams);
        if (query.offset) {
          rethinkQuery = rethinkQuery.slice(query.offset, query.offset + pageSize);
        } else {
          rethinkQuery = rethinkQuery.limit(pageSize);
        }
        rethinkQuery.run(queryResponseHandler);
      }

    } else {
      continueWithPostAccessFilter();
    }
  } else {
    if (this.options.blockPostByDefault) {
      let crudBlockedError = new Error('You are not permitted to perform a CRUD operation on the ' + query.type + ' resource with ID ' + query.id + ' - No access filters found');
      crudBlockedError.name = 'CRUDBlockedError';
      crudBlockedError.type = 'post';
      next(crudBlockedError);
    } else {
      next();
    }
  }
};

module.exports = AccessController;
