(ns clonsq.protocol
  (:require [gloss.core :refer [defcodec enum header ordered-map string]]
            [gloss.io :as io]))

(defcodec frame-types (enum :int32 {:response 0 :error 1 :message 2}))

(defcodec response (ordered-map :type :response
                                :body (string :utf8)))

(defcodec error (ordered-map :type :error
                             :body (string :utf8)))

(defcodec message (ordered-map :type :message
                               :timestamp :int64
                               :attempts :uint16
                               :message-id (string :ascii :length 16)
                               :body (string :utf8)))

(defcodec main
  (ordered-map :size :int32
               :data (header frame-types
                             {:response response
                              :error error
                              :message message}
                             :type)))

(defn decode [buf]
  (io/decode main buf))

(defn encode [data]
  (io/encode main data))
