(ns common-crawl-utils.reader
  (:require [cheshire.core :as json]
            [common-crawl-utils.utils :as utils]
            [clojure.string :as str]
            [common-crawl-utils.constants :as constants]))

(defn get-urls [path]
  (map (partial str constants/cc-s3-base-url)
       (utils/gzip-line-seq (str constants/cc-s3-base-url path))))

(defn get-warc-urls [id]
  (get-urls (format "crawl-data/%s/warc.paths.gz" id)))

(defn read-warc
  ([] (read-warc (get constants/most-recent-crawl :id)))
  ([id] (->> id (get-warc-urls) (mapcat utils/warc-record-seq))))

(defn get-cdx-urls [id]
  (get-urls (format "crawl-data/%s/cc-index.paths.gz" id)))

(defn- parse-coordinate [line]
  (let [[urlkey timestamp body] (str/split line #"\s" 3)]
    (-> body
        (json/parse-string true)
        (assoc :timestamp timestamp
               :urlkey urlkey))))

(defn read-coordinates
  ([] (read-coordinates (get constants/most-recent-crawl :id)))
  ([id] (->> id (get-cdx-urls) (mapcat utils/gzip-line-seq) (map parse-coordinate))))
