const AsyncStreamEmitter = require('async-stream-emitter');

let Cache = function (options) {
  AsyncStreamEmitter.call(this);
  this._cache = {};
  this._watchers = {};
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

Cache.prototype._pushWatcher = function (resourcePath, watcher) {
  if (!this._watchers[resourcePath]) {
    this._watchers[resourcePath] = [];
  }
  this._watchers[resourcePath].push(watcher);
};

Cache.prototype._resolveCacheWatchers = function (resourcePath, data) {
  let watcherList = this._watchers[resourcePath] || [];
  for (let watcher of watcherList) {
    watcher.resolve(data);
  }
  delete this._watchers[resourcePath];
};

Cache.prototype._rejectCacheWatchers = function (resourcePath, error) {
  let watcherList = this._watchers[resourcePath] || [];
  for (let watcher of watcherList) {
    watcher.reject(error);
  }
  delete this._watchers[resourcePath];
};

Cache.prototype.pass = async function (query, provider) {
  if (this.cacheDisabled) {
    return provider();
  }

  let resourcePath = this._getResourcePath(query);
  if (!resourcePath) {
    // Bypass cache for unidentified resources.
    return provider();
  }

  let cacheEntry = this.get(query, resourcePath);

  let watcher = {};
  let promise = new Promise((resolve, reject) => {
    watcher.resolve = resolve;
    watcher.reject = reject;
  });

  this._pushWatcher(resourcePath, watcher);

  if (cacheEntry) {
    this.emit('hit', {
       query,
       cacheEntry
    });
    if (!cacheEntry.pending) {
      // Keep the entry fresh. This will extend its timeout.
      this.set(query, cacheEntry, resourcePath);
      this._resolveCacheWatchers(resourcePath, cacheEntry.resource);
    }
  } else {
    this.emit('miss', {query});
    cacheEntry = {
      pending: true,
      patch: {}
    };

    this.set(query, cacheEntry, resourcePath);
    this.emit('set', {
      query: this._simplifyQuery(query),
      cacheEntry
    });

    (async () => {
      let data;
      try {
        data = await provider();
      } catch (error) {
        this._rejectCacheWatchers(resourcePath, error);
        return;
      }

      let freshCacheEntry = this._cache[resourcePath];
      if (freshCacheEntry && freshCacheEntry.patch) {
        let cacheEntryPatch = freshCacheEntry.patch;
        for (let field of Object.keys(cacheEntryPatch)) {
          data[field] = cacheEntryPatch[field];
        }
      }

      let newCacheEntry = {
        resource: data
      };

      this.set(query, newCacheEntry, resourcePath);
      this.emit('set', {
        query: this._simplifyQuery(query),
        cacheEntry: newCacheEntry
      });

      this._resolveCacheWatchers(resourcePath, data);
    })();
  }

  return promise;
};

Cache.prototype.update = function (query) {
  if (!query.value || typeof query.value !== 'object') return;
  let resourcePath = this._getResourcePath(query);
  if (resourcePath) {
    let cacheEntry = this.get(query, resourcePath);
    if (cacheEntry) {
      if (cacheEntry.pending) {
        for (let [field, value] of Object.entries(query.value)) {
          cacheEntry.patch[field] = value;
        }
      } else {
        for (let [field, value] of Object.entries(query.value)) {
          cacheEntry.resource[field] = value;
        }
      }
      this.emit('update', {
        query,
        cacheEntry
      });
    }
  }
};

module.exports = Cache;
