# Configuring Drafter


Drafter uses [aero](https://github.com/juxt/aero) for its configuration and you can configure it with EDN configuration profiles supplied via the command line, environment variables (and/or java properties).

See the [Configuring Drafter](configuring-drafter.org) (in Emacs org-mode format) document for more information about:
- authentication options
- logging
- environment variables

## Role Based Access Control

When deploying drafter with auth0 auth, users are expected to have been configured with some subset of the following permissions, (depending on what they should be allowed to do), and for those permissions to be passed in the `permissions` claim of the auth token.


```
drafter:draft:claim
drafter:draft:create
drafter:draft:delete
drafter:draft:edit
drafter:draft:publish
drafter:draft:share
drafter:draft:submit
drafter:draft:view
drafter:job:view
drafter:public:view
drafter:user:view
```

How exactly this is done isn't important, and these permissions can be split between roles in a way that makes sense for the specific deployment, but for example you might:

1. Create a new API called PMD, with audience `https://pmd`
2. In <abbr title="Role Based Access Control">RBAC</abbr> Settings, "Enable RBAC" and "Add Permissions in the Access Token"
3. Add all of the above permissions under "Permissions"
4. Authorize the drafter and muttnik "Machine to Machine Applications"
5. Under "User Management" > "Roles" create roles (see below)
6. Assign roles to the relevant users

### Example role mapping:

- PMD-RBAC:User has drafter:public:view
- PMD-RBAC:Reviewer has drafter:draft:view drafter:job:view drafter:public:view
	drafter:user:view
- PMD-RBAC:Editor has drafter:draft:claim drafter:draft:create
	drafter:draft:delete drafter:draft:edit drafter:draft:share
	drafter:draft:submit drafter:draft:view drafter:job:view drafter:public:view
	drafter:user:view
- PMD-RBAC:Publisher has drafter:draft:claim drafter:draft:create
	drafter:draft:delete drafter:draft:edit drafter:draft:publish
	drafter:draft:share drafter:draft:submit drafter:draft:view drafter:job:view
	drafter:public:view drafter:user:view

## Maintenance read-only mode

In production, you can execute the `drafter-toggle-writing.sh` script which communicates with Drafter server over a TCP socket to toggle Drafter into a read-only mode whereby writes (write jobs, or direct calls to `append` etc.) are rejected.

The socket (and hence script) will wait (blocked) until all jobs are flushed, before returning and updating the user with a message when all jobs are flushed. It is then safe to do maintenance. This same information is also written to the drafter log.
