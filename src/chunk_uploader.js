import asyncForever from 'async/forever';
import asyncSeries from 'async/series';
import CryptoJS from 'crypto-js';
import { join as pathJoin } from 'path';
import request from './request';

export default sendChunks;

const REQ_TIMEOUT = 2 * 60 * 1000;
const BACKOFF_RETRY_MS = 60 * 1000;
const INFLIGHT_COUNT = 3;

export function sendChunks(params, done) {
  const {
    file,
    endpoint_url,
    keystone_auth,
    container,
    chunk_list,
    delete_at,
    error_log,
  } = params;

  function progress(fraction, delta) {
    const total_bytes = file.size;
    const total_loaded = chunk_list.reduce(
      (memo, chunk) => memo + chunk.loaded_bytes,
      0
    );
    const total_fraction = total_loaded / total_bytes;
    if (params.progress) {
      params.progress(total_fraction, delta, total_loaded);
    }
  }

  const inflight_count = params.inflight_count || INFLIGHT_COUNT;

  let inflight_list = [];
  const unsent_list = chunk_list.slice();

  let is_complete = false;
  let byte_count = 0;

  function _startSends() {
    if (_isSendDone(chunk_list)) {
      if (!is_complete) {
        is_complete = true;
        done(null, byte_count);
      }
    } else if (inflight_list.length < inflight_count) {
      const chunk = _findNextChunk(unsent_list);
      if (chunk) {
        const opts = {
          endpoint_url,
          keystone_auth,
          container,
          file,
          chunk,
          delete_at,
          error_log,
          progress,
        };
        const chunk_request_number = _sendChunk(opts, _chunkDone);
        inflight_list.push({ chunk_request_number, chunk });
        setTimeout(_startSends, 0);
      }
    }
  }

  function _chunkDone(err, chunk_request_number, chunk) {
    inflight_list = inflight_list.filter(
      (inflight) => inflight.chunk_request_number !== chunk_request_number
    );
    if (!chunk.is_done) {
      unsent_list.push(chunk);
    } else {
      byte_count += chunk.size;
    }
    _startSends();
  }

  _startSends();
}

function _isSendDone(chunk_list) {
  return chunk_list.every((chunk) => chunk.is_done);
}
function _findNextChunk(unsent_list) {
  let ret;
  if (unsent_list.length > 0) {
    ret = unsent_list.pop();
  }
  return ret;
}

let last_request_number = 0;
function _sendChunk(params, done) {
  const {
    endpoint_url,
    keystone_auth,
    container,
    file,
    chunk,
    delete_at,
  } = params;
  const errorLog = params.error_log;
  const chunk_request_number = last_request_number++;
  const slice = file.slice(chunk.start, chunk.end);

  function progress(fraction, delta, loaded) {
    chunk.loaded_bytes = loaded;
    params.progress(fraction, delta, loaded);
  }

  let url;
  asyncSeries(
    [
      (done) => {
        if (chunk.etag) {
          done();
        } else {
          _hashFile(slice, (err, hash) => {
            chunk.etag = hash;
            done(err);
          });
        }
      },
      (done) => {
        if (!chunk.object_path) {
          chunk.object_path = 'segments/' + chunk.etag;
        }
        url = pathJoin(container, chunk.object_path);

        const opts = {
          url,
          etag: chunk.etag,
          endpoint_url,
          keystone_auth,
          delete_at,
          error_log: errorLog,
        };
        _checkChunk(opts, (err, is_done) => {
          if (err) {
            errorLog('check chunk err:', err);
          } else if (is_done) {
            chunk.is_done = true;
            chunk.loaded_bytes = chunk.size;
          } else {
            chunk.loaded_bytes = 0;
          }
          done(err);
        });
      },
      (done) => {
        if (chunk.is_done) {
          done();
        } else {
          const req = {
            url,
            method: 'PUT',
            body: slice,
            headers: {
              Etag: chunk.etag,
            },
            progress,
          };
          if (delete_at) {
            req.headers['X-Delete-At'] = delete_at;
          }
          const opts = {
            endpoint_url,
            keystone_auth,
            req,
          };
          _send(opts, (err, body) => {
            if (err) {
              errorLog('send chunk err:', err, body);
            } else {
              chunk.is_done = true;
              chunk.loaded_bytes = chunk.size;
            }
            done(err);
          });
        }
      },
    ],
    (err) => {
      if (err) {
        // backoff on retry
        setTimeout(() => {
          done(err, chunk_request_number, chunk);
        }, BACKOFF_RETRY_MS);
      } else {
        done(err, chunk_request_number, chunk);
      }
    }
  );

  return chunk_request_number;
}
function _checkChunk(params, done) {
  const {
    url,
    etag,
    endpoint_url,
    keystone_auth,
    delete_at,
    error_log,
  } = params;
  let is_done = false;
  let existing_delete_at;
  asyncSeries(
    [
      (done) => {
        const opts = {
          req: {
            method: 'HEAD',
            url,
          },
          endpoint_url,
          keystone_auth,
        };
        _send(opts, (err, body, response) => {
          if (err === 404) {
            is_done = false;
            err = null;
          } else if (err === 503) {
            error_log(
              '_checkChunk: url:',
              url,
              ', got a 503 treating like 404'
            );
            is_done = false;
            err = null;
          } else if (err) {
            error_log(
              '_checkChunk: url:',
              url,
              'err:',
              err,
              body,
              response && response.headers
            );
          } else if (!err) {
            const existing_etag = response && response.headers.etag;
            if (existing_etag === etag) {
              is_done = true;
              const delete_s = response.headers['x-delete-at'];
              existing_delete_at = parseInt(delete_s || '0');
            }
          }
          done(err);
        });
      },
      (done) => {
        if (!is_done) {
          done();
        } else if (!delete_at && !existing_delete_at) {
          done();
        } else if (existing_delete_at >= delete_at) {
          done();
        } else {
          const opts = {
            req: {
              method: 'POST',
              url,
              headers: {
                'X-Delete-At': delete_at,
              },
            },
            endpoint_url,
            keystone_auth,
          };
          _send(opts, done);
        }
      },
    ],
    (err) => {
      done(err, is_done);
    }
  );
}
function _send(opts, done) {
  const { req, keystone_auth, endpoint_url } = opts;
  if (!req.headers) {
    req.headers = {};
  }
  req.headers['X-Auth-Token'] = keystone_auth;
  req.url = endpoint_url + '/' + req.url;
  req.timeout = REQ_TIMEOUT;

  request(req, done);
}

function _hashFile(file, done) {
  const hash = CryptoJS.algo.MD5.create();
  const reader = file.stream().getReader();
  asyncForever(
    (done) => {
      reader.read().then(
        (result) => {
          const { value } = result;
          if (value && value.length > 0) {
            const word_array = _cryptoJSFixupAB(value);
            hash.update(word_array);
          }
          done(result.done ? 'done' : null);
        },
        (err) => {
          done(err);
        }
      );
    },
    (err) => {
      let digest;
      if (err === 'done') {
        const result = hash.finalize();
        digest = result.toString(CryptoJS.enc.Hex);
        err = null;
      }
      done(err, digest);
    }
  );
}
function _cryptoJSFixupAB(buffer) {
  const temp = [];
  for (var i = 0; i < buffer.length; i += 4) {
    temp.push(
      (buffer[i] << 24) |
        (buffer[i + 1] << 16) |
        (buffer[i + 2] << 8) |
        buffer[i + 3]
    );
  }
  return CryptoJS.lib.WordArray.create(temp, buffer.length);
}
