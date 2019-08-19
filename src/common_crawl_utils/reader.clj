(ns common-crawl-utils.reader
  (:require [cheshire.core :as json]
            [clojure.string :as str]
            [common-crawl-utils.constants :as constants]
            [common-crawl-utils.utils :as utils]))

(defn get-urls [path]
  (map (partial str constants/cc-s3-base-url)
       (utils/gzip-line-seq (str constants/cc-s3-base-url path))))

(defn get-warc-urls [id]
  (get-urls (format "crawl-data/%s/warc.paths.gz" id)))

(defn read-warc
  ([] (read-warc (:id (utils/get-most-recent-crawl))))
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
  ([] (read-coordinates (:id (utils/get-most-recent-crawl))))
  ([id] (->> id (get-cdx-urls) (mapcat utils/gzip-line-seq) (map parse-coordinate))))
