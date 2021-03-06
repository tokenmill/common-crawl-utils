<a href="http://www.tokenmill.lt">
      <img src=".github/tokenmill-logo.svg" width="125" height="125" align="right" />
</a>

# common-crawl-utils

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![pipeline status](https://gitlab.com/tokenmill/oss/common-crawl-utils/badges/master/pipeline.svg)](https://gitlab.com/tokenmill/oss/common-crawl-utils/pipelines/master/latest)
[![Clojars Project](https://img.shields.io/clojars/v/lt.tokenmill/common-crawl-utils.svg)](https://clojars.org/lt.tokenmill/common-crawl-utils)

Various [Common Crawl](https://commoncrawl.org/) utilities in Clojure:

- Fetcher of the selected content from the Common Crawl Archive
- Wrapper on the [Common Crawl Index API](https://index.commoncrawl.org/)
- Reader for the raw Common Crawl WARC archives

## Selected Content Fetcher

```clojure
(common-crawl-utils.fetcher/fetch-content 
  {:cdx-api "https://index.commoncrawl.org/CC-MAIN-2019-09-index" 
   :url "tokenmill.lt" 
   :filter ["status:200"]})
=>
({:offset "272838009",
  :content {:warc "WARC/1.0\r
                   WARC-Type: response\r
                   WARC-Date: 2019-02-17T12:51:41Z\r
                   WARC-Record-ID: <urn:uuid:114c36c3-b278-49bf-b4c0-0cf2a0eaac7c>\r
                   Content-Length: 33486\r
                   Content-Type: application/http; msgtype=response\r
                   WARC-Warcinfo-ID: <urn:uuid:64ce1a6d-7cdc-44ec-a802-f4feac213f0c>\r
                   WARC-Concurrent-To: <urn:uuid:3143fe9c-ec79-4f53-8665-86c668cb46d8>\r
                   WARC-IP-Address: 79.98.31.5\r
                   WARC-Target-URI: http://tokenmill.lt/\r
                   WARC-Payload-Digest: sha1:U3FWVBI7XZ2KVBD72MRR7TCHHXSX2FJS\r
                   WARC-Block-Digest: sha1:MPJRHAU5WNOIDVX2AZTDOEP2YPXXLC66\r
                   WARC-Identified-Payload-Type: text/html",
            :header "HTTP/1.1 200 OK\r
                     Date: Sun, 17 Feb 2019 12:51:41 GMT\r
                     Content-Type: text/html;charset=UTF-8\r
                     X-Crawler-Transfer-Encoding: chunked\r
                     Server: Jetty(7.x.y-SNAPSHOT)",
            :html "THE ACTUAL HTML"},
  :digest "U3FWVBI7XZ2KVBD72MRR7TCHHXSX2FJS",
  :mime "text/html",
  :charset "UTF-8",
  :mime-detected "text/html",
  :filename "crawl-data/CC-MAIN-2019-09/segments/1550247481992.39/warc/CC-MAIN-20190217111746-20190217133746-00381.warc.gz",
  :status "200",
  :urlkey "lt,tokenmill)/",
  :url "http://tokenmill.lt/",
  :length "8548",
  :languages "eng",
  :timestamp "20190217125141"})
```

Uses [Common Crawl Index API](https://index.commoncrawl.org/) to get
coordinates to content which is stored on
[AWS](https://registry.opendata.aws/commoncrawl/).

## Common Crawl Index API Wrapper

Common Crawl Index API wrapper allows to query Common Crawl Index API for
coordinates into the Common Crawl data. 

### Constructing Queries

All queries must contain "url" key. To return all existing coordinates
that match specified host, we append "/*" to it or set key "matchType"
to "host".

```
{:url "tokenmill.lt/*"}

{:url "tokenmill.lt" :matchType "host"}
```

Additionally, results can be filtered. Below is an example, where
coordinates that have "status" containing "200" and "mime" containing
"html" are returned.

```
{:url "tokenmill.lt/*" :filter ["status:200" "mime:html"]}
```

All available fields: *urlkey*, *timestamp*, *url*, *mime*, *status*,
*digest*, *length*, *offset*, *filename*.

Full reference can be found at
[CDX Server API](https://github.com/webrecorder/pywb/wiki/CDX-Server-API).

### Coordinates

Common Crawl is updated on a monthly basis. Each crawl has a specific
index API, which we can query like this:

```
(common-crawl-utils.coordinates/fetch {:cdx-api "https://index.commoncrawl.org/CC-MAIN-2019-09-index" :url "tokenmill.lt" :filter ["status:200"]})
=>
({:offset "272838009",
 :digest "U3FWVBI7XZ2KVBD72MRR7TCHHXSX2FJS",
 :mime "text/html",
 :charset "UTF-8",
 :mime-detected "text/html",
 :filename "crawl-data/CC-MAIN-2019-09/segments/1550247481992.39/warc/CC-MAIN-20190217111746-20190217133746-00381.warc.gz",
 :status "200",
 :urlkey "lt,tokenmill)/",
 :url "http://tokenmill.lt/",
 :length "8548",
 :languages "eng",
 :timestamp "20190217125141"})
```

When "cdx-api" keyword is not specified, most recent one is
used. Currently available index collections can be accessed with
"*common-crawl-utils.utils/get-crawls*" or can be found at:
https://index.commoncrawl.org/collinfo.json

## Reader

Can directly read Common Crawl .warc files containing content, as well
as .cdx files containing coordinates.

```
(common-crawl-utils.reader/read-warc)

(common-crawl-utils.reader/read-coordinates)
```

If no arguments are specified, reads from latest crawl. Otherwise, we
can specify crawl "id" which can be found at
https://index.commoncrawl.org/collinfo.json.

```
(first (common-crawl-utils.reader/read-warc "CC-MAIN-2019-09"))
=>
{:content-length 61992,
  :content-type #object[org.jwat.common.ContentType 0x3eb133dd "application/http; msgtype=response"],
  :date #inst"2019-02-15T19:26:02.000-00:00",
  :filename nil,
  :target-uri #object[org.jwat.common.Uri 0x3efffa13 "http://0204mm.com/?PUT=a_show&AID=68666&FID=1361239&R2=&CHANNEL="],
  :target-uri-str "http://0204mm.com/?PUT=a_show&AID=68666&FID=1361239&R2=&CHANNEL=",
  :warc-type "response",
  :payload-stream #object[org.jwat.common.ByteCountingPushBackInputStream
                          0x1b9da111
                          "org.jwat.common.ByteCountingPushBackInputStream@1b9da111"]}
```

## License

Copyright &copy; 2019 [TokenMill UAB](http://www.tokenmill.lt).

Distributed under the The Apache License, Version 2.0.
