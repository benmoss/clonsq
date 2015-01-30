(ns clonsq.protocol
  (:require [byte-streams :as bs]
            [clojure.string :as string]
            [gloss.core :refer [defcodec enum header ordered-map string]]
            [gloss.io :as io]))

(defcodec frame-types (enum :int32 {:response 0 :error 1 :message 2}))

(defcodec response (ordered-map :type :response
                                :body (string :utf8)))

(defcodec error (ordered-map :type :error
                             :body (string :utf8)))

(defcodec message (ordered-map :type :message
                               :timestamp :int64
                               :attempts :uint16
                               :id (string :ascii :length 16)
                               :body (string :utf8)))

(defcodec main
  (ordered-map :size :int32
               :data (header frame-types
                             {:response response
                              :error error
                              :message message}
                             :type)))

(def wtf (atom []))

(defn decode [buf]
  (let [buf2 (bs/to-byte-buffer buf)]
    (try (io/decode main buf2)
         (catch java.nio.charset.MalformedInputException e
           (swap! wtf conj buf2)))))


(defmulti encode (fn [cmd & more] cmd))
(defmethod encode :nop [_] "NOP\n")
(defmethod encode :fin [_ id] (str "FIN " id "\n"))
(defmethod encode :req [_ id] (str "REQ " id "\n"))
(defmethod encode :magic-id [_] "  V2")
(defmethod encode :subscribe [_ topic channel] (string/join " " ["SUB" topic channel "\n"]))
(defmethod encode :ready [_ n] (string/join " " ["RDY" n "\n"]))
