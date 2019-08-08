(ns common-crawl-utils.coordinates
  (:require [clojure.tools.logging :as log]
            [common-crawl-utils.constants :as constants]
            [common-crawl-utils.utils :as utils]
            [org.httpkit.client :as http]))

(defn get-crawls []
  (utils/request-json constants/index-collinfo))

(defn get-most-recent-crawl []
  (first (get-crawls)))

(defn submit-query [cdx-api query & {:keys [retry-count result-promise] :or {retry-count 0 result-promise (promise)}}]
  (http/request {:url          cdx-api
                 :method       :get
                 :query-params (assoc query :output "json")}
                (fn [{:keys [body error status]}]
                  (cond
                    (= status 404) (deliver result-promise [])
                    (some? error) (if (pos-int? retry-count)
                                    (submit-query cdx-api query (dec retry-count) result-promise)
                                    (do
                                      (log/errorf "Error submitting query `%s` to `%s`: `%s`" query cdx-api error)
                                      (deliver result-promise {:cdx-api cdx-api
                                                               :query   query
                                                               :error   (str error)})))
                    (not= status 200) (if (pos-int? retry-count)
                                        (submit-query cdx-api query (dec retry-count) result-promise)
                                        (do
                                          (log/errorf "HTTP request for query `%s` to `%s` failed with status `%s`" query cdx-api status)
                                          (deliver result-promise {:cdx-api cdx-api
                                                                   :query   query
                                                                   :error   (format "HTTP status `%s`: `%s`" status body)})))
                    :else (deliver result-promise (utils/read-jsonl body)))))
  @result-promise)

(defn get-number-of-pages [cdx-api query]
  (-> (submit-query cdx-api (assoc query :showNumPages true))
      (first)
      (get :pages)))

(defn fetch-pages
  ([cdx-api query number-of-pages]
   (fetch-pages cdx-api query number-of-pages 0))
  ([cdx-api query number-of-pages current-page]
   (when (< current-page number-of-pages)
     (lazy-seq
       (log/debugf "Fetching page `%s` for query `%s` from `%s`" current-page query cdx-api)
       (concat (submit-query cdx-api (assoc query :page current-page))
               (fetch-pages cdx-api query number-of-pages (inc current-page)))))))

(defn fetch
  "Issues HTTP request to Common Crawl Index Server and returns a lazy sequence with content coordinates

  Takes `query` map, which is described in https://github.com/webrecorder/pywb/wiki/CDX-Server-API#api-reference
  and, optionally, `cdx-api` endpoint from https://index.commoncrawl.org/collinfo.json

  If `cdx-api` is not provided, endpoint from most recent crawl is used and also can be accesed
  with (:cdx-api (common-crawl-utils.coordinates/get-most-recent-crawl))

  ;; To fetch all coordinates for host from most recent crawl
  (fetch {:url \"http://www.cnn.com\" :matchType \"host\"})

  ;; To fetch limited number of coordinates
  (take 10 (fetch {:url \"http://www.cnn.com\" :matchType \"host\"}))"
  ([query]
   (fetch query (:cdx-api (get-most-recent-crawl))))
  ([query cdx-api]
   (when-let [number-of-pages (get-number-of-pages cdx-api query)]
     (log/debugf "Total of `%s` pages to fetch for query `%s` from `%s`" number-of-pages query cdx-api)
     (fetch-pages cdx-api query number-of-pages))))
