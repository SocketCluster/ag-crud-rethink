const DOCUMENT_NOT_FOUND_REGEX = new RegExp('^The query did not find a document and returned null.*');
const DUPLICATE_PRIMARY_KEY_REGEX = new RegExp('^Duplicate primary key `(.*)`.*');

function create(rawError) {
  let message = typeof rawError === 'string' ? rawError : rawError.message || 'Unknown error';
  let error = new Error(message);
  if (message.match(DOCUMENT_NOT_FOUND_REGEX)) {
    error.name = 'DocumentNotFoundError';
  } else if (message.match(DUPLICATE_PRIMARY_KEY_REGEX)) {
    error.name = 'DuplicatePrimaryKeyError';
    let primaryKey = message.match(DUPLICATE_PRIMARY_KEY_REGEX)[1];
    if (primaryKey != null) {
      error.primaryKey = primaryKey;
    }
  } else {
    error.name = 'DatabaseError';
  }
  return error;
}

module.exports = {
  create
};
