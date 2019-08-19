(ns common-crawl-utils.utils
  (:require [cheshire.core :as json]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [common-crawl-utils.constants :as constants]
            [org.httpkit.client :as http]
            [warc-clojure.core :as warc])
  (:import (java.io InputStreamReader BufferedReader)
           (java.util.zip GZIPInputStream)
           (java.time Instant)
           (org.jwat.warc WarcReaderFactory)))

(defn gzip-line-seq [path]
  (-> path
      (io/input-stream)
      (GZIPInputStream.)
      (InputStreamReader.)
      (BufferedReader.)
      (line-seq)))

(defn warc-record-seq [path]
  (-> path
      (io/input-stream)
      (WarcReaderFactory/getReader)
      (warc/get-response-records-seq)))

(defn get-http-error [{:keys [body error status]}]
  {:message   (or (some-> error (Throwable->map) (get-in [:via 0 :message]))
                  (format "HTTP status %s: `%s`" status body))
   :timestamp (str (Instant/now))})

(defn request-json [url]
  @(http/request {:url     url
                  :method  :get
                  :timeout constants/http-timeout}
                 (fn [{:keys [error status] :as response}]
                   (cond-> response
                           (= 200 (:status response)) (update :body #(json/decode % true))
                           (or (some? error)
                               (not= status 200)) (update :error (get-http-error response))))))

(defn read-jsonl [s]
  (map #(json/parse-string % true)
       (str/split-lines s)))

(defn get-crawls
  ([]
   (get-crawls constants/index-collinfo))
  ([index-collinfo]
   (let [{:keys [body error] :as response}
         (request-json index-collinfo)]
     (if (some? error)
       (vector response)
       body))))

(defn get-most-recent-crawl
  ([]
   (get-most-recent-crawl constants/index-collinfo))
  ([index-collinfo]
   (first (get-crawls index-collinfo))))
