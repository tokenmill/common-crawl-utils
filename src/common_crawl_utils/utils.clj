(ns common-crawl-utils.utils
  (:require [cheshire.core :as json]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [common-crawl-utils.constants :as constants]
            [org.httpkit.client :as http]
            [warc-clojure.core :as warc])
  (:import (java.io InputStreamReader BufferedReader)
           (java.util.zip GZIPInputStream)
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

(defn request-json [url]
  @(http/request {:url url :method :get}
                 (fn [{:keys [status body]}]
                   (when (= status 200)
                     (json/decode body true)))))

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
