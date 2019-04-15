(ns drafter-swagger-client.api.jobs
  (:require [drafter-swagger-client.core :refer [call-api check-required-params with-collection-format]])
  (:import (java.io File)))

(defn status-finished-jobs-jobid-get-with-http-info
  "Poll to see if asynchronous job has finished
  Poll this route until the AsyncJob is finished.  Whilst the
job is ongoing this route will return a 404 until it is
finished.  When the job finishes, through either successful
completion or an error this route will return a 200 with a
JSON object indicating the success or failure of the task.

The server does not store the set of finished-jobs in
persistent storage, so in exceptional circumstances the set of
finished-jobs may be lost such as after the service has been
restarted.

In order to prevent applications waiting forever for a lost
job to finish, applications should remember and compare
restart-id's after every poll request.

The server assigns itself a new unique restart-id when it is
started, so if an application detects a change in the
restart-id between poll cycles they know that the job they are
awaiting has been lost, and that they should propogate an
appropriate error."
  [jobid ]
  (check-required-params jobid)
  (call-api "/status/finished-jobs/{jobid}" :get
            {:path-params   {"jobid" jobid }
             :header-params {}
             :query-params  {}
             :form-params   {}
             :content-types ["application/json"]
             :accepts       ["application/json"]
             :auth-names    []}))

(defn status-finished-jobs-jobid-get
  "Poll to see if asynchronous job has finished
  Poll this route until the AsyncJob is finished.  Whilst the
job is ongoing this route will return a 404 until it is
finished.  When the job finishes, through either successful
completion or an error this route will return a 200 with a
JSON object indicating the success or failure of the task.

The server does not store the set of finished-jobs in
persistent storage, so in exceptional circumstances the set of
finished-jobs may be lost such as after the service has been
restarted.

In order to prevent applications waiting forever for a lost
job to finish, applications should remember and compare
restart-id's after every poll request.

The server assigns itself a new unique restart-id when it is
started, so if an application detects a change in the
restart-id between poll cycles they know that the job they are
awaiting has been lost, and that they should propogate an
appropriate error."
  [jobid ]
  (:data (status-finished-jobs-jobid-get-with-http-info jobid)))

(defn status-writes-locked-get-with-http-info
  "Poll to see if the system is accepting writes
  During a publish operation operations that create writes such
as creating a draftset and updating it are temporarily
disabled and will cause a 503.

You can poll this route to see if the application is available
for writes.

This route exists to give users & user interfaces information
as to whether the system is available for writes or not.  It
is not necessary (or desirable) for applications to check this
route before performing an operation.  Any operation that
creates writes may 503.

Returns a boolean true if the system is locked for writes and
false if it isn't.'"
  []
  (call-api "/status/writes-locked" :get
            {:path-params   {}
             :header-params {}
             :query-params  {}
             :form-params   {}
             :content-types ["application/json"]
             :accepts       ["application/json"]
             :auth-names    []}))

(defn status-writes-locked-get
  "Poll to see if the system is accepting writes
  During a publish operation operations that create writes such
as creating a draftset and updating it are temporarily
disabled and will cause a 503.

You can poll this route to see if the application is available
for writes.

This route exists to give users & user interfaces information
as to whether the system is available for writes or not.  It
is not necessary (or desirable) for applications to check this
route before performing an operation.  Any operation that
creates writes may 503.

Returns a boolean true if the system is locked for writes and
false if it isn't.'"
  []
  (:data (status-writes-locked-get-with-http-info)))

