(ns common-crawl-utils.fetcher-test
  (:require [clojure.test :refer :all]
            [common-crawl-utils.fetcher :as fetcher]
            [clojure.string :as str]))

(deftest fetcher-test
  (let [cdx-api "http://index.commoncrawl.org/CC-MAIN-2019-09-index"
        query-1 {:url "tokenmill.lt" :filter ["digest:U3FWVBI7XZ2KVBD72MRR7TCHHXSX2FJS"]}
        coordinate-1 {:offset        "272838009",
                      :digest        "U3FWVBI7XZ2KVBD72MRR7TCHHXSX2FJS",
                      :mime          "text/html",
                      :charset       "UTF-8",
                      :mime-detected "text/html",
                      :filename      "crawl-data/CC-MAIN-2019-09/segments/1550247481992.39/warc/CC-MAIN-20190217111746-20190217133746-00381.warc.gz",
                      :status        "200",
                      :urlkey        "lt,tokenmill)/",
                      :url           "http://tokenmill.lt/",
                      :length        "8548",
                      :languages     "eng",
                      :timestamp     "20190217125141"}]
    (testing "Query submission"
      (are [query response] (= response (fetcher/submit-query query cdx-api))
                            query-1 [coordinate-1]))
    (testing "Page counting"
      (are [query response] (= response (fetcher/get-number-of-pages query cdx-api))
                            {:url "tokenmill.lt/*"} 1
                            {:url "-"} 0))
    (testing "Coordinate fetching"
      (are [query response] (= response (fetcher/fetch-coordinates query cdx-api))
                            query-1 [coordinate-1]))
    (testing "Fetching single coordinate content"
      (are [query coordinates] (let [response (fetcher/fetch-content query :cdx-api cdx-api)]
                                 (and (= coordinates (map #(dissoc % :content) response))
                                      (every? #(not (str/blank? (-> % (get :content) (vals) (str/join "\r\n\r\n")))) response)))
                               query-1 [coordinate-1]))
    (testing "Fetching content"
      (is (pos-int? (count (fetcher/fetch-content query-1 :cdx-api cdx-api)))))))
