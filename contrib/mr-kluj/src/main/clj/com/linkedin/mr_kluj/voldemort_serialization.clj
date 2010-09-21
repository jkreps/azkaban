;Copyright 2010 LinkedIn, Inc
;
;Licensed under the Apache License, Version 2.0 (the "License"); you may not
;use this file except in compliance with the License. You may obtain a copy of
;the License at
;
;http://www.apache.org/licenses/LICENSE-2.0
;
;Unless required by applicable law or agreed to in writing, software
;distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
;WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
;License for the specific language governing permissions and limitations under
;the License.

(ns com.linkedin.mr-kluj.voldemort-serialization
  (:refer-clojure :exclude [group-by])
  (:require
    [clojure.contrib.str-utils2 :as str-utils]
    [com.linkedin.mr-kluj.job :as job]
    [com.linkedin.mr-kluj.utils :as utils]
    [com.linkedin.mr-kluj.hadoop-utils :as hadoop-utils]
    )
  (:import
    [com.linkedin.json LatestExpansionFunction JsonSequenceFileOutputFormat JsonSequenceFileInputFormat]

    [java.lang RuntimeException]

    [org.apache.hadoop.fs Path FileSystem FileStatus]
    [org.apache.hadoop.conf Configuration]
    [org.apache.hadoop.io BytesWritable Text SequenceFile$Reader]
    [org.apache.hadoop.mapreduce Job MapContext]
    [org.apache.hadoop.mapreduce.lib.input FileInputFormat]
    [org.apache.hadoop.mapreduce.lib.output FileOutputFormat]

    [org.apache.log4j Logger]

    [voldemort.serialization.json JsonTypeSerializer]))

(def *logger* (Logger/getLogger "voldemort-storage"))

(defn make-schema
  "Converts a [[field-name data-type] ...] sequence into a json schema"
  [fields]
  (format "{%s}" (str-utils/join ", " (map (fn [[name data-type]] (format "'%s':%s" name data-type)) fields))))


(let [config (Configuration.)
      fs (FileSystem/get config)]
  (defn get-schema
    "Gets the voldemort storage schema for a given path"
    [path]
    (let [sequence-file-reader (SequenceFile$Reader. fs (first (hadoop-utils/spider-path fs path)) config)
          key-schema (-> sequence-file-reader (.getMetadata) (.get (Text. "key.schema")) (.toString))
          val-schema (-> sequence-file-reader (.getMetadata) (.get (Text. "value.schema")) (.toString))]
      [key-schema val-schema])))

(let [config (Configuration.)
      fs (FileSystem/get config)
      latest-expansion-function (LatestExpansionFunction. fs *logger*)
      latest-exp-fn (fn [path] (.apply latest-expansion-function path))]
  (defn voldemort-storage-input
    "Defines the input for a job.

    path should be the path to a file or directory of files that are Sequence files serialized using voldemort serialization."
    [path]
    (when (nil? path) (throw (RuntimeException. "Input path cannot be null.")))
    (job/add-config
      (fn [#^Job job]
        (let [actual-paths (str-utils/join "," (map latest-exp-fn (str-utils/split path #",")))]
          (.info #^Logger *logger* (format "Voldemort Input: Given paths[%s] which resolved to paths[%s]" path actual-paths))
          (doto job
            (.setInputFormatClass JsonSequenceFileInputFormat)
            (FileInputFormat/addInputPaths #^String actual-paths)))))))

(defn voldemort-storage-output
  "Defines the output for a job to use voldemort storage.

  key-schema and value-schema are String representations of the schemas of the output data."
  [path #^String key-schema #^String value-schema]
  (job/add-config
    (fn [#^Job job]
      (when (nil? path) (throw (RuntimeException. (format "Output on job[%s] cannot be null." (.getJobName job)))))
      (JsonTypeSerializer. key-schema) ; Verify that a serializer can be created at job creation time.
      (JsonTypeSerializer. value-schema)
      (doto job
        (.setOutputKeyClass BytesWritable)
        (.setOutputValueClass BytesWritable)
        (.setOutputFormatClass JsonSequenceFileOutputFormat)
        (FileOutputFormat/setOutputPath (Path. path)))
      (doto (.getConfiguration job)
        (.set "output.key.schema" key-schema)
        (.set "output.value.schema" value-schema))
      job)))

(defn voldemort-intermediate-data
  "Sets up the mapper and reducer for using voldemort serialization as the intermediate data serialization scheme.

  Takes the String representation of the key schema and value schema as arguments and does the necessary bits to the
  starter, mapper and reducer such that it will be used."
  [#^String key-schema #^String value-schema]
  (let [key-serializer (JsonTypeSerializer. key-schema)
        value-serializer (JsonTypeSerializer. value-schema)]
    (job/intermediate-serialization
      (fn [#^Job job]
        (doto job
          (.setMapOutputKeyClass BytesWritable)
          (.setMapOutputValueClass BytesWritable)))
      (fn [key value context]
        [(BytesWritable. (.toBytes key-serializer key)) (BytesWritable. (.toBytes value-serializer value))])
      (fn [#^BytesWritable key values context]
        [(.toObject key-serializer (.getBytes key)) (map (fn [#^BytesWritable val] (.toObject value-serializer (.getBytes val))) values)]))))

(defn group-by [fields projections]
  "Performs a group-by using voldemort serialization for the intermediate serialization format.

  fields is a [[field-name data-type] ...] sequence
  projections is a [[name default-fn accumulator-fn data-type] ...] sequence"
  (let [field-names (map (fn [[field-name serialization-type]] field-name) fields)
        key-schema (format "{%s}" (str-utils/join ", " (map (fn [[field-name serialization-type]] (format "'%s':%s" field-name serialization-type)) fields)))
        value-schema (format "{%s}" (str-utils/join ", " (map (fn [[name default-fn acc-fn data-type]] (format "'%s':%s" name data-type)) projections)))
        aggregation-fn
        (fn [key values context]
          [[key
            (clojure.core/reduce
              (fn [old-val new-val]
                (clojure.core/reduce conj {}
                  (map (fn [[name default-fn acc-fn]] {name (acc-fn (get old-val name) (get new-val name))}) projections)))
              values)]])]
    (job/compose-wrappers
      (job/map-reduce-output (fn [key value context] [[false (conj {} key value)]]))
      (voldemort-intermediate-data key-schema value-schema)
      (job/create-reducer aggregation-fn)
      (job/create-combiner aggregation-fn)
      (job/use-combiner)
      (job/map-mapper
        (fn [key value context]
          [[(select-keys value field-names)
            (clojure.core/reduce conj {} (map (fn [[name default-fn acc-fn]] {name (default-fn value)}) projections))]])))))

(defn group-by-no-combine [fields projections]
  "Performs a group-by using voldemort serialization for the intermediate serialization format.

  fields is a [[field-name data-type] ...] sequence
  projections is a [[name default-fn accumulator-fn data-type] ...] sequence"
  (let [field-names (map (fn [[field-name serialization-type]] field-name) fields)
        key-schema (format "{%s}" (str-utils/join ", " (map (fn [[field-name serialization-type]] (format "'%s':%s" field-name serialization-type)) fields)))
        value-schema (format "{%s}" (str-utils/join ", " (map (fn [[name default-fn acc-fn data-type]] (format "'%s':%s" name data-type)) projections)))
        aggregation-fn
        (fn [key values context]
          [[key
            (clojure.core/reduce
              (fn [old-val new-val]
                (clojure.core/reduce conj {}
                  (map (fn [[name default-fn acc-fn]] {name (acc-fn (get old-val name) (get new-val name))}) projections)))
              values)]])]
    (job/compose-wrappers
      (job/map-reduce-output (fn [key value context] [[false (conj {} key value)]]))
      (voldemort-intermediate-data key-schema value-schema)
      (job/create-reducer aggregation-fn)
      (job/map-mapper
        (fn [key value context]
          [[(select-keys value field-names)
            (clojure.core/reduce conj {} (map (fn [[name default-fn acc-fn]] {name (default-fn value)}) projections))]])))))

(defn- join-sorted
  ([key-fn value-fn merge-fn previously-joined coll] (join-sorted key-fn value-fn merge-fn previously-joined coll 0 []))
  ([key-fn value-fn merge-fn previously-joined coll index curr-joined]
    (if (empty? coll)
      curr-joined
      (let [curr-element (first coll)
            curr-key (key-fn curr-element)]
        (if (= curr-key index)
          (recur key-fn value-fn merge-fn previously-joined (rest coll) index (concat curr-joined (map merge-fn previously-joined (repeat (value-fn curr-element)))))
          (recur key-fn value-fn merge-fn curr-joined coll curr-key []))))))

(defn join
  "Performs a join among the inputs using the fields specified in the fields grouping

  fields is a [[field-name data-type] ...] sequence
  inputs is a [[input-path input-serializer-fn projections out-value-schema & wrappers] ...] sequence
    input-serializer-fn is a function that takes a single argument (the path) and returns a wrapper that will
      add whatever is required to the job configuration.  Note that this configuration should be orthogonal
      to any configuration that other input formats might do, as a join implies that multiple inputs will be touching
      the job configuration.
    projections is a [[field-name data-type] ...] sequence"
  [fields inputs]
  (let [field-names (map (fn [[field-name serialization-type]] field-name) fields)
        key-schema (format "{%s}" (str-utils/join ", " (map (fn [[field-name serialization-type]] (format "'%s':%s" field-name serialization-type)) fields)))]
    (job/compose-wrappers
      (voldemort-intermediate-data key-schema "{'index':'int8', 'value':'bytes'}")
      (job/wrap-reducer-input
        (let [deserializer-map (into {} (map vector (iterate inc 0) (map (comp #(JsonTypeSerializer. #^String %) make-schema #(% 2)) inputs)))]
          (fn [key values context]
            [key
             (map (fn [{:strs [index value]}] {:index index :value (.toObject #^JsonTypeSerializer (deserializer-map (int index)) #^bytes value)})
               values)])))
      (job/wrap-reducer-input
        (fn [key values context]
          [key (sort-by :index values)]))
      (job/wrap-reducer-input
        (fn [key values context]
          [false (join-sorted :index :value conj [(into {} key)] values)]))
      (job/wrap-reducer-input
        (let [output-keys (concat (mapcat (fn [[path serializer projections]] (map first projections)) inputs) (map first fields))
              force-output-fn (utils/make-select-all-fn output-keys)]
          (fn [key values context]
            [key (map force-output-fn values)])))
      (job/mapper-wrapper
        (fn [mapper-fn]
          (let [current-path
                (-> #^MapContext user/*context* (.getInputSplit) (.getPath) (.toUri) (.getPath))
                [index [input-path input-serializer-fn projections & wrappers]]
                (first (filter (fn [[index [input-path]]] (.startsWith #^String current-path input-path)) (map vector (iterate inc 0) inputs)))
                projection-field-names
                (map (fn [[field-name]] field-name) projections)
                out-value-schema
                #^String (make-schema projections)
                output-serializer
                (JsonTypeSerializer. out-value-schema)
                actual-wrappers
                (job/compose-wrappers
                  (job/map-mapper (fn [key value context] [[key {"index" (byte index) "value" (.toBytes output-serializer value)}]]))
                  (job/map-mapper
                    (let [select-projections-fn (utils/make-select-all-fn projection-field-names)]
                      (fn [key value context]
                        [[(select-keys value field-names) (select-projections-fn value)]])))
                  (apply job/compose-wrappers wrappers))]
            (println (format "Voldemort Join: working on file[%s] which is join index[%s]" current-path index))
            (job/wrap-mapper actual-wrappers mapper-fn))))
      (reduce job/compose-wrappers (map (fn [[path serializer-fn]] (serializer-fn path)) inputs)))))

(defn join-reduce
  "Composes a reducer (ala com.linkedin.mr_kluj.job/create-reducer) and a join, allowing you to specify functionality
  on the joined group that can see all the values as if it was grouped by the join key"
  [join-wrapper reduce-fn]
  (job/compose-wrappers join-wrapper (job/create-reducer reduce-fn)))