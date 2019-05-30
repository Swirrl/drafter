(ns dev
  (:require [martian.core :as martian]
            [martian.clj-http :as martian-http]
            [clojure.pprint :refer [pprint]]
            [clojure.string :as string]
            [clojure.walk :as walk]
            [drafter-client.client :as client]
            [drafter-client.client-test :refer [drafter-client]]))

(defn gen-api [client]
  `(do
     ~@(for [[k desc] (sort (distinct (martian/explore client)))]
         (let [api-desc (:parameters (martian/explore client k))
               args (map first (filter (comp keyword? first) api-desc))
               opts (->> api-desc
                         (remove (comp keyword? first))
                         (map (comp :k first)))
               sym (comp symbol name)
               argm (into {} (map (juxt identity sym) args))]
           `(defn ~(symbol (name k)) ~desc
              {:drafter-client.client.impl/generated true}
              [~'client
               ~@(map sym args)
               ~@(when (seq opts)
                   ['& {:keys (mapv sym opts) :as 'opts}])]
              (martian/response-for ~'client ~k
                                    ~(if (seq opts) `(merge ~argm ~'opts) argm)))))))

(defmacro def-api
  "`def`ine functions directly calling the remote `api` with
  `martian/response-for`"
  []
  (gen-api (drafter-client)))

(comment

  (gen-api (drafter-client))

  (def-api)


  (-> "http://localhost:3001"
      (client/web-client :batch-size 0)

      )

  )
