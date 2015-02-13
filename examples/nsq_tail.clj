(ns nsq-tail
  (:require [byte-streams :as bs]
            [clojure.string :as string]
            [clojure.tools.cli :refer [parse-opts]]
            [clonsq.consumer :as consumer]))

(def messages-shown (atom 0))

(defn exit [status msg]
  (println msg)
  (System/exit status))

(defn handler [total-messages conn msg]
  (swap! messages-shown inc)
  (println (bs/to-string (:body msg)))
  (consumer/finish! conn (:id msg))
  (when (and (> total-messages 0)
             (>= @messages-shown total-messages))
    (exit 0 "--total-messages reached")))

(defn run [{:keys [lookupd-http-address topic channel max-in-flight total-messages]}]
  (println (str "Connecting to nsqds.."))
  (consumer/create {:lookupd-http-address lookupd-http-address
                    :topic topic
                    :channel channel
                    :max-in-flight (Integer/parseInt max-in-flight)
                    :handler (partial handler (Integer/parseInt total-messages))}))

;;;; cli setup ;;;;

(def cli-options
  (let [parse-int #(Integer/parseInt %)]
    [["-t" "--topic TOPIC" "(required) NSQ topic"]
     ["-m" "--max-in-flight COUNT" "max number of messages to allow in flight"
      :default 200
      :parse-fn parse-int]
     ["-l" "--lookupd-http-address ADDRESS" "(required) lookupd HTTP address (may be given multiple times)"
      :default []
      :default-desc ""
      :assoc-fn (fn [m k v] (assoc m k (conj (k m) v)))]
     ["-n" "--total-messages COUNT" "total messages to show (will wait if starved)"
      :default 0
      :parse-fn parse-int]
     ["-c" "--channel CHANNEL" "NSQ channel"
      :default (str "nsq-tail" (-> (Math/random) (* 100000) int) "#ephemeral")]
     ["-h" "--help"]]))

(def required-options
  #{:topic :lookupd-http-address})

(defn missing-opts [opts]
  (into {} (filter (fn [[k v]] (and (required-options k)
                                    (not (seq v))))
                   opts)))

(defn error-msg [errors summary]
  (str "The following required fields were missing:\n\n"
       (string/join \newline (keys errors))
       "\n\nUsage:\n\n"
       summary))

(defn -main [& args]
  (let [{:keys [options arguments summary errors]} (parse-opts args cli-options)
        missing (missing-opts options)]
    (cond
      (:help options) (exit 0 summary)
      (seq missing) (exit 1 (error-msg missing summary)))
    (run options)))
