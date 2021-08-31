export default request;

export function request(args, done) {
  function request_done(...args) {
    if (done) {
      done(...args);
      done = null;
    }
  }

  const { method, url, progress } = args;
  const default_headers = {};
  let body = null;
  if (_isValidBody(args.body)) {
    body = args.body;
  } else if (args.body) {
    body = JSON.stringify(args.body);
    if (args.force_text) {
      default_headers['Content-Type'] = 'text/plain';
    } else {
      default_headers['Content-Type'] = 'application/json';
    }
  }

  const headers = Object.assign({}, default_headers, args.headers);
  const xhr = new XMLHttpRequest();
  if (args.timeout) {
    xhr.timeout = args.timeout;
  }
  xhr.onload = () => {
    let status = xhr.status === 1223 ? 204 : xhr.status;
    let ret_body = false;
    let json = false;
    let err = null;

    ret_body = xhr.response || xhr.responseText;
    const content_type = xhr.getResponseHeader('Content-Type');
    const is_json = content_type && content_type.indexOf('json') !== -1;

    if (ret_body && ret_body.length && is_json) {
      try {
        json = JSON.parse(ret_body);
      } catch (e) {
        err = e;
      }
    }

    if (status < 100 || status > 599) {
      err = 'bad_status';
    } else if (status > 399) {
      err = status;
    }

    const response = { headers: {} };
    const header_text = xhr.getAllResponseHeaders() || '';
    header_text.split('\n').forEach((line) => {
      const k_v = line.split(': ');
      if (k_v.length === 2) {
        response.headers[k_v[0].toLowerCase().trim()] = k_v[1].trim();
      }
    });
    request_done(err, json || ret_body, response);
  };
  xhr.onerror = () => {
    request_done('xhr_error');
  };
  xhr.ontimeout = () => {
    request_done('timeout');
  };
  xhr.open(method, url, true);
  for (let name in headers) {
    xhr.setRequestHeader(name, headers[name]);
  }

  let last_loaded = 0;
  if (progress) {
    xhr.upload.onprogress = (e) => {
      if (e.lengthComputable) {
        const delta = e.loaded - last_loaded;
        const fraction = e.loaded / e.total;
        last_loaded = e.loaded;
        progress(fraction, delta, e.loaded);
      }
    };
  }
  xhr.send(body);
}
function _isValidBody(body) {
  return (
    body instanceof FormData || (body && typeof body.arrayBuffer === 'function')
  );
}
