(ns clj-spark-apps.core
  (:require [clojure.string :as string]
            [sparkling.conf :as conf]
            [sparkling.core :as spark]
            [sparkling.destructuring :as s-de]
            [sparkling.serialization])
  ;(:require [amazonica.aws.s3 :as s3])
  (:import [scala Tuple2]
           [org.apache.spark.api.java Optional]
	   [org.apache.spark.api.java JavaSparkContext]
	   [com.amazonaws.auth DefaultAWSCredentialsProviderChain]	   
           [com.amazonaws.services.s3 AmazonS3 AmazonS3Client]
           [com.amazonaws.services.s3.model ListObjectsRequest
                                           ; ListObjectsV2Request
                                           ; ListObjectsV2Result
                                            ObjectListing
                                            S3ObjectSummary])
	   ;[com.amazonaws.services.s3 S3CredentialsProviderChain])
  (:gen-class))


(def special #{"," "." "!" "?" "(" ")" "[" "]" "{" "}"})

(def prune (re-pattern (format "[%s]" (reduce str (map #(str "\\" %) special)))))

(def stopwords #{"a"    "all" "and"  "any"
                 "are"  "is"  "in"   "of"
                 "on"   "or"  "our"  "so"
                 "this" "the" "that"
                 "to"   "we"})

(def remove-stopwords (partial remove (partial contains? stopwords)))

;; --------------
;; -   CONFIG   -
;; --------------

(defn get-env [k]
  (System/getenv k))

(defn get-input []
  (get-env "INPUT_SOURCE"))

(defn get-output []
  (get-env "OUTPUT_DEST"))

(defn set-hadoop [ctx k v]
  (println (format "setting key[%s]" k))
  (.set (.hadoopConfiguration ctx) k v))

(defn configure-common [ctx]
  (set-hadoop ctx "fs.s3a.access.key"    (get-env "AWS_ACCESS_KEY_ID"))
  (set-hadoop ctx "fs.s3a.secret.key"    (get-env "AWS_SECRET_ACCESS_KEY")))  

(defn configure-from-role [ctx]
  (configure-common ctx)
  (set-hadoop ctx "fs.s3a.aws.credentials.provider" "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")
  (set-hadoop ctx "fs.s3a.session.token" (get-env "AWS_SESSION_TOKEN")))

(defn configure [ctx]
  (configure-common ctx))

(defn ^JavaSparkContext spark-context []
  (let [ctx (-> (conf/spark-conf)
                (conf/master "local[*]")
                (conf/app-name "tf-idf")
                spark/spark-context)]
    (if (= "role" (get-env "AUTH_TYPE"))
      (configure-from-role ctx)
      (configure ctx))
    ctx))

;; -------------------
;; -     UTIL        -
;; -------------------

#_(defn -list-objs [bucket prefix]
  (s3/list-objects-v2
    {:bucket-name bucket
     :prefix prefix}))

;; TODO: optionally provide alternative creds provider
(defn ^AmazonS3Client s3-client []
  (AmazonS3Client. (DefaultAWSCredentialsProviderChain.)))

#_(defn ^ListObjectsV2Result -list-objs [bucket prefix]
  (let [clnt (s3-client)
        req (-> (ListObjectsV2Request.)
                (.withBucketName bucket)
		(.withPrefix prefix))]
    (.listObjectsV2 clnt req)))

(defn -list-objs [bucket prefix]
  (let [clnt (s3-client)
        req (-> (ListObjectsRequest.)
                (.withBucketName bucket)
		(.withPrefix prefix))]
    (.listObjects clnt req)))

(defn list-objs [bucket prefix]
  (let [ret (-list-objs bucket prefix)
        filename #(format "s3a://%s/%s" bucket %)]
    (clojure.pprint/pprint ret)
    (loop [xs (.getObjectSummaries ret)
           acc []]
      (if-let [^S3ObjectSummary obj (first xs)]
        (recur (rest xs) (conj acc (filename (.getKey obj))))
        (rest acc)))))

#_(defn list-objs [bucket prefix]
)

(defn get-sources [input]
  (let [[bucket key] (rest (re-find #"s3://([\w\_\-]+)/(.+)$" input))]
    (list-objs bucket key)))

;; TODO: make generic function for source file loading
(defn s3->rdd [sc & files]
  (println (format "FILES=%s"files))
  (loop [xs files
         acc []]
    (if-let [f (first xs)]
      (let [rdd (spark/text-file sc f)]
        (println rdd)
	(recur (rest xs) (conj acc rdd)))
      acc)))

(defn local->rdd [sc & files]
  (let [source (partial format "/tmp/%s")]
    (loop [xs files
           acc []]
      (if-let [f (first xs)]
        (let [rdd (spark/text-file sc (source f))]
          (println rdd)
	  (recur (rest xs) (conj acc rdd)))
        acc))))

(defn cleaner [line]
  (string/replace line prune ""))

(defn clean-rdds [rdds]
  (loop [xs rdds
         acc []]
    (if-let [rdd (first xs)]
      (recur (rest xs)
             (conj acc (->> rdd
                            (spark/map cleaner)
			    (spark/filter #(not= % "")))))
      (do
        (println (format "CLEAN-RDDS=%s" acc))
	acc))))


;; ---------------
;; -   PARSERS   -
;; ---------------

(declare parse-tup)
(declare parse-seq)
(declare parse-opt)

(defn noop [x]
  ;(println (format "PASSTHRU -> %s" x))
  x)

;; # TODO: add pull request to sparkling to add other join support (full join)
(defn joiner [acc rdd]
  (.fullOuterJoin acc rdd))

(defn get-opt-val [o]
  (if (.isPresent o) (.get o)))
   
(defn tup? [x]
  (= x Tuple2))

(defn opt? [x]
  (= x Optional))

(defn unpack [x]
  (let [t (type x)]
    (cond
      (tup? t) (parse-tup x)
      (seq? x) (parse-seq x)
      (opt? t) (parse-opt x)
      :else    (noop x))))

;; Functions for unpacking tuple2/optional sequences

(defn parse-tup [x]
  ;(println (format "PARSE TUPLE -> %s" x))
  (let [k (._1 x)
        v (._2 x)]
    ;(println (format "KEY=%s" k))
    ;(println (format "VAL=%s" v))
    [(unpack k) (unpack v)]))

(defn parse-seq [x]
  ;(println (format "PARSE SEQUENCE -> %s" x))
  (loop [xs x
         acc []]
    (if-let [entry (first xs)]
      (recur (rest xs) (conj acc (unpack entry)))
      acc)))

(defn parse-opt [x]
  ;(println (format "PARSE OPTIONAL -> %s" x))
  [(unpack (get-opt-val x))])


;; -------------------
;; -     TF-IDF      -
;; -------------------

(defn text->rdds [sc sources]
  (loop [xs sources
         acc []]
    (if-let [src (first xs)]
      (recur (rest xs) (conj acc (spark/parallelize-pairs sc src)))
      acc)))

;; Transform RDD of lines into (k,1) for each word then aggregate on k

(defn line-splitter [line]
  (loop [xs (string/split line #" ")
         acc []]
    (if-let [token (first xs)]
      (recur (rest xs) (conj acc (spark/tuple (string/lower-case token) 1)))
      acc)))

(defn line->keys [sc doc]
  (println (format "TYPE=%s" (type doc)))
  (->> doc
       (spark/flat-map-to-pair line-splitter)
       spark/sort-by-key
       spark/cache))

(defn doc-term-counts [sc rdd]
  (->> rdd
       (spark/reduce-by-key +)  
       spark/collect))

(defn doc-total-term-count [sc rdd]
  (->> rdd
       (spark/count)
       spark/collect))

(defn term-frequency [sc rdd sum]
  (->> rdd
       (spark/reduce-by-key +)
       (spark/map-values #(/ % sum))))

(defn idf-smooth [N n]
  (Math/log10 (+ 1 (/ N n))))

(defn idf [N n]
  (Math/log10 (/ N n)))

;; TODO: clean up this function
(defn -term-doc-freq [sc N rdds]
  (let [ret  (->> (reduce joiner rdds)
                  (spark/map-values unpack)
                  (spark/map-values #(->> % flatten (remove nil?) count))
                  (spark/map-values #(idf N %))
                  spark/cache)]
    ret))

(defn inverse-document-frequency [sc rdds]
  (-term-doc-freq sc (count rdds) rdds))

(defn fname [prefix idx]
  (format "%s/rdd-%d" prefix idx))

(defn save [output rdds]
  (let [prefix (format "%s/%d" output (System/currentTimeMillis))]
    (doseq [[rdd i] (map list rdds (range (count rdds)))]
      (let [path (fname prefix i)]
        (println (format "***** SAVING RDD[%s] %d *****" rdd i))
        (->> rdd
            (spark/repartition 1)
            (spark/save-as-text-file path))))))

(defn compute-tf-idf [tfs idf]
  (loop [xs tfs
         i 1
         acc []]
    (if-let [tf (first xs)]
      (do
        (println (format "COMPUTING TF-IDF FOR RDD[%d]" i))
        (recur (rest xs)
               (inc i)
               (conj acc (->> (spark/join tf idf)
                              (spark/map-values #(* (._1 %) (._2 %)))
			      spark/sort-by-key))))
      acc)))

(defn run-tf-idf [sc output args]
  (let [rdds (clean-rdds (apply s3->rdd sc args))
        N    (count rdds)]
    (println (format"IDF-TEST[N=%s]" N))
    (println (format "IDF=%s" (idf N 1)))
    (println (format "IDF=%s" (idf N 2)))    
    (loop [xs rdds
           acc []
           i 0]
      (if-let [rdd (first xs)]
        (let [keys (line->keys sc rdd)
              sum  (spark/count keys)
	      tf   (term-frequency sc keys sum)]
	  (clojure.pprint/pprint (spark/collect tf))
	  (recur (rest xs) (conj acc tf) (inc i)))
	(let [idfs (inverse-document-frequency sc acc)]
          (save output (compute-tf-idf acc idfs)))))))


;; -------------
;; -   TEST    -
;; -------------

;; --- SET 1 ---
(def foo [(spark/tuple "a" 1) (spark/tuple "b" 2) (spark/tuple "c" 3)])
(def bar [(spark/tuple "a" 11) (spark/tuple "d" 14) (spark/tuple "e" 15)])
(def baz [(spark/tuple "a" 111) (spark/tuple "g" 107) (spark/tuple "c" 103)])
(def oof [(spark/tuple "a" 1111) (spark/tuple "b" 1002) (spark/tuple "h" 1008)])


;; --- SET 2 ---
(def rab "This is a a sample")
(def zab "This is another another example example example")

;; --- DRIVERS ---
(defn multi-join-test [sc & sources]
  (let [rdds (text->rdds sc sources)
        ret  (->> (reduce joiner rdds)
                  (spark/map-values unpack)
                  (spark/map-values #(->> % flatten (remove nil?) count))
                  spark/collect)]
    (println (format "LENGTH=%d" (count ret)))
    ret))

;; =============
;; -    MAIN   -
;; =============


(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (let [sc (spark-context)]
    (println (format "INPUT=%s" (get-input)))
    (println (format "OUTPUT=%s" (get-output)))
    #_(clojure.pprint/pprint (get-sources (get-input)))
    (clojure.pprint/pprint (run-tf-idf sc (get-output) (get-sources (get-input))))))
