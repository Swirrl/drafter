(ns drafter.user)

(def roles #{:editor :publisher :manager})
(defrecord User [email role api-key-digest])

(defn create-user [email role api-key-digest]
  {:pre [(contains? roles role)]}
  (->User email role api-key-digest))

(defn has-role? [{:keys [role] :as user} requested]
  {:pre [(contains? roles requested)]}
  (case role
    :manager true
    :publisher (not= requested :manager)
    :editor (= requested :editor)))
