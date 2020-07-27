# Drafter Update!

## Noted issues and responses

### Jena SSE doesn’t support building update queries

I spent an hour or so messing around with this, this morning. It works, there's
one small sidestep to get the classes to line up, but it's fine.

### Knowledge of graphs affected

We need to know which graphs _would be_ affected by an update, so that:

1. We can clone them into the draftset
2. Update metadata about them in drafter

#### Enforce specified graphs

We _could_ ensure that we know which graphs are affected by only allowing update
statements that specify graphs.

#### Magic

Come up with a way of knowing this, that's adequately performant.

## Use Cases

> The case that we’re trying to solve in muttnik is when you want to ‘replace’
> some data transactionally (e.g. for metadata).

This is not the only case. There have been a few times I've thought "this would
be simpler with an UPDATE". Mainly DELETE by pattern.

### Opinion

I'm not sure it's worth doing a half-arsed solution just for the case of
UPSERTing some metadata a few times in muttnik. The reason we said "we might as
well do a full UPDATE endpoint properly" was because the half-arsed workaround
took ages longer than expected and is still bad.

Some version of UPDATE is more generally useful than just for these cases, but
could take longer than it's worth.

## What does a Drafter Update API look like?

> So, instead of an actual sparql update endpoint it might be best to make a new
> drafter api which is a “drafter update.“? i.e. where you can send
> triples/quads/graph-uris to delete and triples to add to happen in one
> external job. At the moment we only have delete and add separately.

Equivalent to:

``` sparql
DELETE DATA ... ;
INSERT DATA ... ;
```

> The alternative is to add start/end transaction semantics, but that might be
> complicated. Just allowing a single op for ‘replace’ would be enough (and
> simpler)?

More-or-less the same as above in the smallest case.

> Because these jobs are async, we just want one job to track in mutt really,
> though the alternative would be to allow a better way to track multiple jobs
> in mutt i guess…

Potentially more work?

### Limited SPARQL UPDATE

Could we just limit the SPARQL UPDATE to what we can easily support? And extend
as/if we understand we can?

Apache Jena parses the `UpdateRequest` into a list of `Update`[1] objects, which
end up being one of a few subclasses. We could easily limit which subclasses we
support.

This also has the benefit of being executed as one request, so _should_ fail as
a whole statement if one part fails.

[1] https://jena.apache.org/documentation/javadoc/arq/org/apache/jena/update/Update.html

#### `UpdateData`

The case previously mentioned, should be easy.

``` sparql
DELETE DATA ... ;
INSERT DATA ... ;
```

#### `UpdateDeleteWhere`

Equivalent to:

``` sparql
DELETE WHERE { ... }
```

Takes a list of Quad patterns. Would be easy to restrict to quad with graphs
specified?

Or, is it not effectively a `SELECT` then `DELETE DATA`? It _should_ then be
possible to:

1. `SELECT` quads in drafter
2. Work out which graphs are involved
3. Perform require graph actions (copy/delayed update meta?)
4. Delete quads

#### `UpdateDeleteInsert`

I.E.,

``` sparql

DELETE { ... }
INSERT { ... }
WHERE  { ... }
```

Sub-sub-class, but potentially what we mean when talking about `UPDATE` or
`UPSERT`.

This is a bit harder to have knowledge of which graphs are involved. We can
probably also restrict to specified graphs, but is it then generally useful?

> This operation identifies data with the WHERE clause, which will be used to
> compute solution sequences of bindings for a set of variables. The bindings
> for each solution are then substituted into the DELETE template to remove
> triples, and then in the INSERT template to create new triples. [1]

Could we:

1. Run the `WHERE` clause as a `SELECT`
2. Work out which graphs are involved
3. Perform require graph actions (copy/delayed update meta?)
4. Substitute bindings in the `DELETE` and `INSERT` templates for each solution

[1] https://www.w3.org/TR/sparql11-update/#deleteInsert

#### Other Update Types

Most of the other Update types seem trivial to implement. However, `UpdateLoad`
seems like it would probably be problematic.

# Choices

Choices, choices, choices...

Obviously, if we're attempting to implement some part of a SPARQL update
endpoint, we'd like to keep the semantics the same.

But, what do we do when a DELETE DATA/INSERT DATA request is large? Do we:

* hold up writes for an indeterminate amount of time
* say no
* split it up/batch

The problem being with batching it, at least how batching currently works, is
that now we now diverge from an update request being atomic. If we batch, the
DELETE may succeed but the INSERT not. (Unless we can wrangle a transaction
across async jobs - which seems potentially leaky.) We also need to sync the
async job, so that the HTTP request can return the correct codes, etc., not that
that's too much of a big deal.

I'm a little concerned about rewriting a big update with the current SSE
strategy too. The jena modify/Update API is apparently designed with large
INSERT DATA type requests in mind, but I'm not sure about the SSE stuff, and
certainly not the way we currently convert to strings & back. To be fair, SSE is
only focusing on one quad at once, so maybe it's OK, but rebuilding the Query
might need some work to work in a memory efficient way.

The "easy" path then is to not allow "big" updates, at least for the
moment. This would still be good for the few uses in muttnik around
delete/insert, but I'm not sure if still worth it for a now doubly limited
endpoint.

If we could branch off a draft (a draft of a draft? as we've fancifully
discussed before), then we could "solve" the atomicity issue, by using the
branch like a transaction, and dropping it if there's a failure in the batched
job.

Ideally, to solve the update rewriting on memory busting data, we'd need a
streaming SPARQL parser, where we could rewrite as part of a
stream->parse->partition/batch->rewrite->unparse->stream pipeline.
