var rethink = require("rethinkdb");

rethink.connect({host: '54.197.161.29', port: 28015}, function (err, conn) {
  if (err) {
    throw err;
  }
  
  console.log(777);
  //init(conn);
});

// TODO: When attaching - Make sure that the connection is opened
// before the plugin module begins servicing requests
module.exports.attach = function (scServer, socket, options) {
  options = options || {};
  
  if (!options.pageSize) {
    options.pageSize = 10;
  }
  
  var init = function (connection) {
    socket.on('get', function (query, callback) {
      var deepKey = [query.type];
      if (query.id) {
        deepKey.push(query.id);
        
        if (query.field) {
          deepKey.push(query.field);
        }
      }
      
      var dataHandler = function (err, data) {
        if (query.id) {
          callback(err, data);
        } else {
          var resultAsArray = [];
          for (var i in data) {
            if (data.hasOwnProperty(i)) {
              resultAsArray.push(data[i]);
            }
          }
          callback(err, resultAsArray);
        }
      };
      
      if (query.page == null) {
        scServer.global.get(deepKey, dataHandler);
      } else {
        var startIndex = query.page * options.pageSize;
        scServer.global.getRange(deepKey, startIndex, startIndex + options.pageSize, dataHandler);
      }
    });
    
    socket.on('set', function (query, callback) {
      var deepKey = [query.type, query.id];
      if (query.field) {
        deepKey.push(query.field);
      }
      scServer.global.set(deepKey, query.value, function (err) {
        if (!err) {
          if (query.field) {
            var publishPacket = {
              type: 'set',
              value: query.value
            };
            scServer.global.publish(query.type + '/' + query.id + '/' + query.field, publishPacket);
          }
        }
        callback(err);
      });
    });
    
    socket.on('add', function (query, callback) {
      var deepKey = [query.type, query.id, query.field];
      
      var queryFn = function (DataMap) {
        var foreignKey = DataMap.get(foreignKeyPath);
        if (foreignKey) {
          var collectionLength = DataMap.count(deepKey);
          var nextId = DataMap.count(foreignKey) + 1;
          
          DataMap.add(deepKey, nextId);
          
          var pageNumber = Math.floor(collectionLength / pageSize);
          var pageIndex = collectionLength % pageSize;
          
          return {
            pageNumber: pageNumber,
            index: pageIndex,
            value: nextId
          };
        } else {
          return {
            error: 'Invalid foreign key'
          };
        }
      };
    
      queryFn.data = {
        deepKey: deepKey,
        pageSize: options.pageSize,
        foreignKeyPath: ['Schema', query.type, 'foreignKeys', query.field]
      };
      
      scServer.global.run(queryFn, function (err, result) {
        if (result.error) {
          callback('Collection in field ' + query.field + ' of type ' + query.type + ' is not associated with a valid foreign key');
        } else {
          if (!err) {
            var publishPacket = {
              type: 'add',
              index: result.index,
              value: result.value,
              pageSize: options.pageSize
            };
            
            scServer.global.publish(result.pageNumber + ':' + query.type + '/' + query.id + '/' + query.field, publishPacket);
          }
          callback(err, result.value);
        }
      });
    });
  };
};