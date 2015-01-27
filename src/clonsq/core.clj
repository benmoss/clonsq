(ns clonsq.core
  (:require [aleph.tcp :as tcp]
            [aleph.http :as http]
            [byte-streams :as bs]
            [manifold.deferred :as d]
            [cheshire.core :as json]))

(defn lookup [lookupd-host topic]
  (d/chain (http/get (str lookupd-host "/lookup?topic=" topic))
           :body
           bs/to-string
           json/parse-string))


(comment
  (deref (lookup "http://localhost:4161" "test")))
