# VERA setup preflight

Run only these endpoint checks before changing project files.

1. Accept the two supplied absolute URLs. Remove trailing slashes, reject embedded
   credentials, queries, and fragments, and require HTTPS except for loopback.
2. Call `${VERA_API_URL}/health/live` and `${VERA_API_URL}/health/ready` without
   credentials. Both responses must be successful.
3. Make one short, non-streaming `OPTIONS` or `HEAD` request to `VERA_MCP_URL`.
   Any HTTP response below 500 confirms that the endpoint is reachable; `401` or
   `403` means runtime authentication is still required. A timeout, connection
   failure, `404`, or 5xx response fails preflight.
