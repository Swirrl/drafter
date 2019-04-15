(ns drafter.test-helpers.lock-manager
  "A test helper for managing ReentrantLock states in tests.  Works by
  spawning a thread to lock/release the Lock and using promises to
  instruct it on the transitions to take and when."
  (:import java.util.concurrent.TimeUnit))

(defn take-lock! [{:keys [lock-request locked] :as lock-mgr}]
  (deliver lock-request :ok)
  @locked)

(defn release-lock! [{:keys [locked release-request released] :as lock-mgr}]
  (deliver release-request :ok)
  @released)

(defn build-lock-manager
  "This function is intended to make testing the behaviour of the global-writes-lock easier.

  build-lock-manager takes a ReentrantLock and provides a lock-manager
  for coordinating state transitions on the lock,
  i.e. locked/unlocked.

  The lock-manager is initialised in the lock state, calling the
  release-lock! function on the lock-manager will then release the
  lock before returning control.

  This is necessary because locks are managed thread locally, so we
  internally spawn a thread and use promises to coordinate state
  transitions on the lock from locked to released.

  This function will raise an assertion error if the provided lock is
  locked.

  Lock managers can only be used once, and cycle through the states:

  1. unlocked-waiting-for-lock-request (start)
  2. locked-waiting-for-release-request
  3. unlocked (finish)

  "
  [the-lock]

  (let [locked? (.isLocked the-lock)]
    (assert (not locked?)
            "The global writes lock is locked.  For the tests to
          function properly it must've been released.  If you're
          seeing this there is probably a bug in the test suite."))
  (let [lock-request (promise)
        locked (promise)
        release-request (promise)
        released (promise)
        lock-mgr {:the-lock the-lock
                  :lock-request lock-request
                  :locked locked
                  :release-request release-request
                  :released released}]

    (future
      @lock-request ;; wait for request to engage lock

      (try
        (if (.tryLock the-lock 1 TimeUnit/SECONDS)
          (do
            (deliver locked :ok))
          (do
            ;; let the other threads know our start state was unexpected
            (deliver (:locked lock-mgr) :test-failed-to-acquire-lock)
            (deliver (:released lock-mgr) :test-failed-to-acquire-lock)
            ;; kill this thread
            (assert false "Failed to acquire lock needed for test, it was expected to be free.  This is likely a bug in your test suite")))

        @release-request
        (finally
          (.unlock the-lock)
          (deliver released :ok))))
    lock-mgr))
