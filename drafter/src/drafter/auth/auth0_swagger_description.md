OAuth2 bearer token authentication via Auth0. API users can be configured in the Auth0 management console
with a drafter username and collection of scopes mapped to drafter roles. After obtaining a bearer token from
the Auth0 tenant token endpoint, these can be provided on a request as an `Authorization` header with the format `Bearer <token>`.

When authenticating via the UI, the application `client_id` and `client_secret` should be specified along with the
collection of scopes to request for the token.