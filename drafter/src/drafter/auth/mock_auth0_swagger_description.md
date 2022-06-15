__Warning__: This authentication method should only be used in development

Signed JWT bearer token authentication via Auth0. User tokens can be constructed by assigning the required issuer, audience, username
and required scopes, and then signing the resulting document with the private key of the configured RSA keypair. The constructed
token should then be supplied on the request with an `Authorization` header of the form `Bearer <token>`.

When authenticating via the UI, the `Bearer` prefix must also be supplied e.g.

__Value__:

    Bearer eyJ0eXAiOiJKV1QiL ...


