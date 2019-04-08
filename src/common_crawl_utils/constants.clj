(ns common-crawl-utils.constants
  (:require [common-crawl-utils.utils :as utils]
            [clojure.java.io :as io]))

(def config
  (utils/read-edn (io/resource "config.edn")))

(def cc-s3-base-url
  (get config :cc-s3-base-url))

(def crawls
  @(utils/request-json (get config :index-collinfo)))

(def most-recent-crawl
  (first crawls))
