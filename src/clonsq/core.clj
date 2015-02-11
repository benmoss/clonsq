(ns clonsq.core
  (:require [aleph.http :as http]
            [byte-streams :as bs]
            [cheshire.core :as json]
            [clonsq.consumer :as c]
            [clonsq.protocol :as proto]
            [manifold.deferred :as d]
            [manifold.stream :as s])
  (:import (java.util.concurrent TimeoutException)))

(defn lookup [topic lookupd-host]
  (let [lookupd-address (str lookupd-host "/lookup?topic=" topic)]
    (-> (d/timeout! (http/get lookupd-address) 200)
        (d/chain :body
                 bs/to-string
                 json/parse-string)
        (d/catch TimeoutException
          (fn [e] {:error e :address lookupd-address})))))

(defn print-errors [errors]
  (doseq [e errors]
    (println (format "error querying nsqlookupd (%s)"
                     (:address e) (:error e)))))

(defn connect [{:keys [lookupd-http-address topic] :as opts}]
  (let [addresses (if (sequential? lookupd-http-address)
                    lookupd-http-address
                    (list lookupd-http-address))
        {errors true
         responses false} (->> addresses
                               (map (partial lookup topic))
                               (apply d/zip)
                               deref
                               (group-by #(contains? % :error)))
        producers (mapcat #(get-in % ["data" "producers"]) responses)]
    (print-errors errors)
    (c/create producers opts)))

(defn finish! [sink id]
  (s/put! sink (proto/encode :fin id)))

(defn close! [consumer]
  (doseq [conn @(:connections consumer)
          :let [streams (:streams conn)]]
    (s/put! (:sink streams) (proto/encode :close))
    (dorun (map #(s/close! %) (vals streams)))))

(comment
  (defn handler [sink msg]
    (finish! sink (:id msg)))

  (def consumer (connect {:lookupd-http-address ["http://localhost:4161"
                                                 "http://localhost:5161"]
                          :topic "test"
                          :channel "test"
                          :max-in-flight 200
                          :handler #'handler}))
  )
