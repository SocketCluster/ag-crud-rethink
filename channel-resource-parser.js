let channelViewParamsRegex = /^([^\(]*)\((.*)\):([^:]*)$/;
let crudChannelRegex = /^crud>(.*)$/;

module.exports.parseChannelResourceQuery = function (channelName) {
  if (typeof channelName !== 'string') {
    return null;
  }
  let channelMatches = channelName.match(crudChannelRegex);
  if (channelMatches && channelMatches[1]) {
    let resourceString = channelMatches[1];

    if (resourceString.indexOf(':') !== -1) {
      // If resource is a view.
      let viewMatches = resourceString.match(channelViewParamsRegex);
      if (!viewMatches) return null;

      let viewResource = {
        view: viewMatches[1],
        type: viewMatches[3]
      }
      try {
        viewResource.viewParams = JSON.parse(viewMatches[2]);
      } catch (e) {}

      return viewResource;
    }
    // If resource is a simple model.
    let resourceParts = resourceString.split('/');
    let modelResource = {
      type: resourceParts[0]
    };
    if (resourceParts[1]) {
      modelResource.id = resourceParts[1];
    }
    if (resourceParts[2]) {
      modelResource.field = resourceParts[2];
    }
    return modelResource;
  }
  return null;
};
