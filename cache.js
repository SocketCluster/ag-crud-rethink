const AsyncStreamEmitter = require('async-stream-emitter');

let Cache = function (options) {
  AsyncStreamEmitter.call(this);
  this._cache = {};
  this.options = options || {};
  this.cacheDuration = this.options.cacheDuration || 10000;
  this.cacheDisabled = !!this.options.cacheDisabled;
};

Cache.prototype = Object.create(AsyncStreamEmitter.prototype);

Cache.prototype._getResourcePath = function (query) {
  if (!query.type || !query.id) {
    return null;
  }
  return query.type + '/' + query.id;
};

Cache.prototype._simplifyQuery = function (query) {
  return {
    type: query.type,
    id: query.id
  };
};

Cache.prototype.set = function (query, data, resourcePath) {
  if (!resourcePath) {
    resourcePath = this._getResourcePath(query);
  }
  let entry = {
    resource: data
  };

  let existingCache = this._cache[resourcePath];
  if (existingCache && existingCache.timeout) {
    clearTimeout(existingCache.timeout);
  }

  entry.timeout = setTimeout(() => {
    let oldCacheEntry = this._cache[resourcePath] || {};
    delete this._cache[resourcePath];
    this.emit('expire', {
      query: this._simplifyQuery(query),
      oldCacheEntry
    });
  }, this.cacheDuration);

  this._cache[resourcePath] = entry;
};

Cache.prototype.clear = function (query) {
  let resourcePath = this._getResourcePath(query);

  let oldCacheEntry = this._cache[resourcePath];
  if (oldCacheEntry) {
    if (oldCacheEntry.timeout) {
      clearTimeout(oldCacheEntry.timeout);
    }
    delete this._cache[resourcePath];
    this.emit('clear', {
      query: this._simplifyQuery(query),
      oldCacheEntry
    });
  }
};

Cache.prototype.get = function (query, resourcePath) {
  if (!resourcePath) {
    resourcePath = this._getResourcePath(query);
  }
  let entry = this._cache[resourcePath] || {};
  return entry.resource;
};

Cache.prototype.update = function (query) {
  let resourcePath = this._getResourcePath(query);
  if (resourcePath) {
    let cacheEntry = this.get(query, resourcePath);
    if (cacheEntry) {
      let data;
      if (query.field) {
        data = {[query.field]: query.value};
      } else {
        data = query.value;
        if (!data || typeof data !== 'object') {
          return;
        }
      }
      for (let [field, value] of Object.entries(data)) {
        cacheEntry.resource[field] = value;
      }
      this.emit('update', {
        query,
        cacheEntry
      });
    }
  }
};

module.exports = Cache;
