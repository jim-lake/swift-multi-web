import asyncRetry from 'async/retry';
import asyncSeries from 'async/series';
import { join as pathJoin } from 'path';
import request from './request';

import { fetchAuth } from './auth';
import { sendChunks } from './chunk_uploader';

const CHUNK_SIZE = 100 * 1024 * 1024;
const REQ_TIMEOUT = 10 * 60 * 1000;
const SEND_RETRY_COUNT = 5;

export default sendFile;

export function sendFile(params, done) {
  const {
    os_auth_url,
    os_password,
    os_username,
    os_tenant_name,
    file,
    container,
    object_path,
    inflight_count,
    delete_at,
    progress,
  } = params;
  const errorLog = params.error_log || function () {};
  const consoleLog = params.console_log || function () {};

  const file_size = file.size;
  const chunk_list = [];
  for (let pos = 0, i = 0; pos < file_size; i++) {
    const size_left = file_size - pos;
    const size = Math.min(size_left, CHUNK_SIZE);
    chunk_list.push({
      index: i,
      size,
      start: pos,
      end: pos + size,
      loaded_bytes: 0,
    });
    pos += size;
  }
  if (chunk_list.length === 1) {
    chunk_list[0].object_path = object_path;
  }
  consoleLog(
    'Uploading:',
    file.name,
    '=>',
    container + '/' + object_path,
    'size:',
    file_size,
    'chunks:',
    chunk_list.length
  );

  let endpoint_url;
  let keystone_auth;
  let byte_count = 0;
  asyncSeries(
    [
      (done) => {
        const opts = {
          os_auth_url,
          os_password,
          os_username,
          os_tenant_name,
        };
        fetchAuth(opts, (err, token, service_map) => {
          if (err) {
            errorLog('fetchAuth: failed err:', err);
          } else {
            keystone_auth = token;
            if (service_map.swift && service_map.swift[0]) {
              endpoint_url = service_map.swift[0].publicURL;
            }
            if (!endpoint_url) {
              errorLog('no swift url');
              err = 'no_endpoint';
            }
          }
          done(err);
        });
      },
      (done) => {
        const opts = {
          keystone_auth,
          endpoint_url,
          file,
          container,
          chunk_list,
          inflight_count,
          delete_at,
          error_log: errorLog,
          progress,
        };
        sendChunks(opts, (err, count) => {
          if (err) {
            errorLog('send_chunks failed, err:', err);
          } else {
            byte_count = count;
          }
          done(err);
        });
      },
      (done) => {
        if (chunk_list.length === 1) {
          done();
        } else {
          const slo_content = chunk_list.map((chunk) => {
            return {
              path: pathJoin(container, chunk.object_path),
              etag: chunk.etag,
              size_bytes: chunk.size,
            };
          });
          const req = {
            method: 'PUT',
            url: `${container}/${object_path}?multipart-manifest=put`,
            body: slo_content,
            json: true,
            headers: {},
          };
          if (delete_at) {
            req.headers['X-Delete-At'] = delete_at;
          }
          const opts = { req, endpoint_url, keystone_auth };
          _sendRetry(opts, (err, body) => {
            if (err) {
              errorLog('create_slo err:', err, body);
            }
            done(err);
          });
        }
      },
    ],
    (err) => {
      done(err, byte_count);
    }
  );
}

function _sendRetry(params, done) {
  const opts = {
    times: SEND_RETRY_COUNT,
    interval: (count) => {
      const interval = 50 * Math.pow(3, count);
      return interval;
    },
  };
  asyncRetry(opts, (done) => _send(params, done), done);
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
