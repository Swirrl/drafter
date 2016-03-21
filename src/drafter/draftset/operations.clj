(ns drafter.draftset.operations
  (:require [drafter.draftset :as ds]
            [drafter.user :as user]))

(defn- set-submitter [draftset submitter]
  (assoc :submitted-by (user/username submitter)))

(defn submit-to-role [draftset submitter role]
  (-> draftset
      (dissoc :current-owner)
      (assoc :claim-role role)
      (set-submitter submitter)))

(comment  (defn submit-to-user [draftset submitter target]
            (-> draftset
                (assoc :current-owner (user/username target))
                (set-submitter submitter))))

(defn claim [draftset claimant]
  (-> draftset
      (dissoc :claim-role)
      (assoc :current-owner claimant)))

(def operations #{:delete :edit :submit :publish :claim})
