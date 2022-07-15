The [Basic Authentication](https://en.wikipedia.org/wiki/Basic_access_authentication) scheme is defined by [RFC 7617](https://datatracker.ietf.org/doc/html/rfc7617). Authentication information is added to a request
under the `Authorization` header with the value `Basic Base64({username}:{password})` i.e. the literal string 'Basic'
followed by the base64 encoding of the user's username and password separated by a single `:` character.

For example a user `admin@opendatacommunities.org` could attempt
to authenticate every request to the service described here by
prefixing the Base64 encoded value of
`admin@opendatacommunities.org:yourpmdapikey` with the string
`Basic` followed by a space. For example:

````
Authorization: Basic YWRtaW5Ab3BlbmRhdGFjb21tdW5pdGllcy5vcmc6eW91cnBtZGFwaWtleQ==
````

The username and password fields can be supplied directly in the corresponding form fields when authenticating
through the UI.