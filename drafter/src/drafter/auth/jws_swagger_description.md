Represents user authentication information as a token contained as a payload within a [JSON Web Signature (JWS)](https://datatracker.ietf.org/doc/html/rfc7515).
Tokens must be signed with an appropriate key and contain the required claims to be accepted as valid. 

Token values should be added to a request as an `Authorization` header with the format `Token <token>` where `<token>` contains the
signed token value.

When authenticating via the UI, the `Token` prefix must also be specified e.g.

__Value:__

    Token eyJhbGciOiJIUzI ...
    
Human users are not usually expected to authenticate using this mechanism.