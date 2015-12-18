# DraftSet Data Model

`PREFIX : <http://publishmydata.com/def/drafter/>`

## Ownership

At creation a draftset's `:state` property is set to `:drafting`, and
the draftset's `:current-owner` is set to the user who created it.

A draftset can have at most 1 `:current-owner`; additionally though
the `:current-owner` can also be unset, in which case any user in the
system can set the `:current-owner` to any other user in the system.

A draftsets data and metadata can only ever be modified by the
draftsets `:current-owner`.  If a draftset has no `:current-owner` it
will be unmodifiable by anyone until its `:current-owner` is set.

The exception to this rule is that users with the `:reviewer` role can
set or unset any draftsets `:current-owner`, and a draftset with no
`:current-owner` can have its `:current-owner` set to any user, by any
user.

Whenever a change of the `:current-owner` occurs prior to making the
change the system copies the prior `:current-owner` to a
`:previous-owner` property, in order to allow reviewers to reject a
draftset, and return it to the `:previous-owner` for modification.

In the future we may look at preserving a chain of owners and
replacing the above mechanism with something more in line with the
provo vocabulary.

## Reviewing

A draftset can be submitted for review, this is done by the draftsets
owner setting the draftset's `:state` property to `:reviewing`.

Once in a `:reviewing` state the draftset is made visible to users
with the `reviewer` role.  If the `:current-owner` who submitted it
does not have the `:reviewer` role it is no longer modifiable by them;
however so long as they remain the `:current-owner` they can reset the
state back to `:drafting`, to re-enable their editing abilities and
remove it from the list of draftsets available to reviewers.

A reviewer wishing to modify the draftset must then set themselves to
the `:current-owner` before making changes or publishing the draftset.

## Publishing

When a reviewer publishes the draftset the system sets the draftset to
the transitory `:publishing` state in order to deny any further
modifications to it whilst the publishing operation happens.  Once the
draftset is published the data within it is removed.

The record of the draftset may in the future be stored in a
`:published` or `:archived` state to retain the provenance and history
of the changes.
