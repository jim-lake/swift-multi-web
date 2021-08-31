import request from './request';

const REQ_TIMEOUT = 10 * 1000;

export default fetchAuth;

export function fetchAuth(params, done) {
  const { os_auth_url, os_password, os_username, os_tenant_name } = params;
  const opts = {
    url: os_auth_url + 'tokens',
    method: 'POST',
    body: {
      auth: {
        passwordCredentials: {
          password: os_password,
          username: os_username,
        },
        tenantName: os_tenant_name,
      },
    },
    timeout: REQ_TIMEOUT,
  };
  request(opts, (err, body) => {
    let token_id;
    const service_map = {};
    if (!err && body) {
      const access = body.access;
      const token = access && access.token;
      token_id = token && token.id;
      const serviceCatalog = access && access.serviceCatalog;
      if (serviceCatalog && serviceCatalog.length > 0) {
        serviceCatalog.forEach((service) => {
          const { name, endpoints } = service;
          service_map[name] = endpoints;
        });
      }
    }
    done(err, token_id, service_map);
  });
}
