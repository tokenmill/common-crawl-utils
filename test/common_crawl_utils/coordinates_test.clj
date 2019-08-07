(ns common-crawl-utils.coordinates-test
  (:require [clojure.test :refer :all]
            [common-crawl-utils.coordinates :as coordinates]))

(deftest ^:integration coordinates-test
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
      (are [query response] (= response (coordinates/submit-query cdx-api query))
                            query-1 [coordinate-1]))
    (testing "Page counting"
      (are [query response] (= response (coordinates/get-number-of-pages cdx-api query))
                            {:url "tokenmill.lt/*"} 1
                            {:url "-"} 0))
    (testing "Coordinate fetching"
      (are [query response] (= response (coordinates/fetch query cdx-api))
                            query-1 [coordinate-1]))
    (testing "Fething single coordinate for multi page result"
      (is (some? (first (coordinates/fetch {:url "delfi.lt" :matchType "host"} cdx-api)))))))
