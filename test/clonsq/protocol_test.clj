(ns clonsq.protocol-test
  (:require [byte-streams :as bs]
            [clojure.test :refer :all]
            [clonsq.protocol :as proto]
            [manifold.stream :as s]))

(defn body->string [msg-seq]
  (map (fn [{body :body :as msg}]
         (assoc msg :body (bs/to-string body)))
       msg-seq))

(deftest decode
  (testing "Decoding a channel containing a message"
    (let [bytes [0 0 0 41 0 0 0 2 19 -66 -113 100 -42 -127 -120 -28 0 1 48 55 99 57 100 54 55 100 101 49 97 57 98 55 101 57 104 101 108 108 111 32 119 111 114 108 100]
          stream (s/->source [(byte-array bytes)])]
      (is (= (-> stream
                 proto/decode-stream
                 s/stream->seq
                 body->string)
             '({:body "hello world",
                :id "07c9d67de1a9b7e9",
                :attempts 1,
                :timestamp 1422732195553970404,
                :type :message}))))
  (testing "Decoding a channel containing two messages"
    (let [bytes [0 0 0 41 0 0 0 2 19 -66 -112 39 87 -43 20 59 0 1 48 55 99 57 100 54 55 100 101 49 97 57 98 55 102 51 104 101 108 108 111 32 119 111 114 108 100
                 0 0 0 39 0 0 0 2 19 -66 -112 39 -126 44 -13 -64 0 1 48 55 99 57 100 54 55 100 101 49 97 57 98 55 102 52 98 121 101 32 119 111 114 108 100]
          stream (s/->source [(byte-array bytes)])]
      (is (= (->> stream
                 proto/decode-stream
                 s/stream->seq
                 body->string
                 (map :body))
             '("hello world" "bye world")))))))
