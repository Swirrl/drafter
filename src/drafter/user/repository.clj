(ns drafter.user.repository
  (:require [clojure.tools.logging :as log]))

(defprotocol UserRepository
  (find-user-by-email-address [this email]
    "Attempts to find a user with the given email address (user name)
    in the underlying store. Returns nil if no such user was found."))

(defn- get-repository-ns-name [env-map]
  (let [repo-ns (:drafter-user-repo-ns env-map)
        default-ns-name "drafter.user.memory-repository"]
    (if (some? repo-ns)
      repo-ns
      (do
        (log/warn "No user repository namespace configured, using default: " default-ns-name)
        (log/warn "To configure the user repository, set the DRAFTER_USER_REPO_NS environment variable")
        default-ns-name))))

(defn get-configured-repository [env-map]
  (let [repo-ns-name (symbol (get-repository-ns-name env-map))]
    (require repo-ns-name)
    (if-let [repo-ns (find-ns repo-ns-name)]
      (if-let [get-repo-fn-var (.findInternedVar repo-ns 'get-repository)]
        (@get-repo-fn-var env-map)
        (throw (RuntimeException. (str "No var get-repository found in namespace " repo-ns-name))))
      (throw (RuntimeException. (str "Cannot find namespace " repo-ns-name))))))
