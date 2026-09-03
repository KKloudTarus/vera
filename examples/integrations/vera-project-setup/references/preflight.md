# VERA setup preflight

Run only these endpoint and authentication checks before changing project files.

1. Accept the two supplied absolute URLs. Remove trailing slashes, reject embedded
   credentials, queries, and fragments, and require HTTPS except for loopback.
   `VERA_MCP_URL` must already include a path ending in `/mcp`; do not append the
   path or substitute `VERA_API_URL`.
2. Call `${VERA_API_URL}/health/live` and `${VERA_API_URL}/health/ready` without
   credentials. Both responses must be successful.
3. Send one unauthenticated JSON-RPC `initialize` request to `VERA_MCP_URL`. A
   successful response selects no-auth mode regardless of host. A timeout,
   connection failure, `404`, or 5xx response fails preflight.
4. If the unauthenticated request returns `401` or `403`, read its
   `WWW-Authenticate` protected-resource metadata URL. Require HTTPS and the same
   origin as `VERA_MCP_URL`, then validate the metadata's `resource` and
   `authorization_servers`. Fetch the advertised authorization-server metadata
   and select OAuth only when it exposes usable authorization and token endpoints
   plus PKCE `S256`. A metadata pointer alone does not prove OAuth works.
5. If OAuth discovery is valid, use the selected runtime's interactive OAuth
   login and request the four coding scopes. The runtime must own secure token
   storage and refresh; never copy its access token into project config.
6. If OAuth discovery or interactive login fails, retain any existing JWT config
   and prepare the selected runtime's untracked config with exactly one
   `<VERA_MCP_JWT>` placeholder. Ask the user to run `install_jwt.py` with the API
   URL, MCP URL, and config path. The helper requests the four coding scopes,
   validates `access_token`, `expires_in`, and `Cache-Control: no-store`, performs
   authenticated JSON-RPC `initialize`, and atomically replaces the placeholder.
   Use `--existing-token` when the user already has an MCP JWT.

Never ask for, read, or print the REST API key or MCP JWT. Never use the API key as
the MCP bearer token. Do not run a config-inspection command that prints static
headers.
