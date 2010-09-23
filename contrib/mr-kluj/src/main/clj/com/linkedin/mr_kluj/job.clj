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
 
(ns com.linkedin.mr-kluj.job
  (:refer-clojure :exclude [group-by filter])
  (:require
    [clojure.contrib.str-utils2 :as str-utils]
    [com.linkedin.mr-kluj.hadoop-utils :as hadoop-utils]
    )
  (:import
    [com.linkedin.mr_kluj GenericClojureJob HiddenFilePathFilter StagedOutputJob ClojureMapper ClojureReducer ClojureCombiner]
    [com.linkedin.json LatestExpansionFunction]

    [java.lang RuntimeException]
    [java.util Properties HashMap Map]

    [org.apache.hadoop.fs Path FileSystem]
    [org.apache.hadoop.conf Configuration]
    [org.apache.hadoop.io Text BytesWritable LongWritable]
    [org.apache.hadoop.mapreduce MapContext ReduceContext Job]
    [org.apache.hadoop.mapreduce.lib.input FileInputFormat TextInputFormat]
    [org.apache.hadoop.mapreduce.lib.output FileOutputFormat TextOutputFormat]

    [org.apache.log4j Logger]
    ))

(def *logger* (Logger/getLogger "job.clj"))
(declare set-job-config)

(defprotocol HadoopJob
  "Everything required to run a Hadoop Job"
  (starter [this]
    "Returns a (fn [] ...) that returns a org.apache.hadoop.mapreduce.Job object")
  (mapper [this]
    "Returns a (fn [key value context] ...) that acts as a mapper.
    The mapper should return a [[key value] ...] of the key/value pairs that are the output of the Hadoop Mapper.")
  (combiner [this]
    "Returns a (fn [key values context] ...) that acts as a combiner.
    The combiner should return a [[key value] ...] of the key/value pairs to be output by the Hadoop Combiner.")
  (reducer [this]
    "Returns a (fn [key values context] ...) that acts as a reducer.
    The reducer should return a [[key value] ...] of the key/value pairs to be output by the Hadoop Reducer."))

(defprotocol HadoopJobWrapper
  "A HadoopJobWrapper is a wrapper object that affects some part, or parts, of a HadoopJob.

  The idea behind wrappers is that they form an algebra of mutations on a base HadoopJob.  The composition of
  wrappers gets applied to a HadoopJob object to yield a final, working hadoop job.

  This allows for the expression of common operations like filters, selection, joining and grouping to be reused,
  while still providing the flexibility to allow for custom map reduce code to be injected into the mix."
  (wrap-starter [this starter-fn]
    "A call to (wrap-starter a-hadoop-job-wrapper (fn [] ...)) will produce a new
    (fn [] ...) that wraps the initial function to do extra stuff.

    It is acceptable for this method to do nothing and just return starter-fn." )
  (wrap-mapper [this mapper-fn]
    "A call to (wrap-mapper a-hadoop-job-wrapper (fn [key value context] ...)) will produce a new
    (fn [key value context] ...) that wraps the initial function to do extra stuff.

    It is acceptable for this method to do nothing and just return mapper-fn.")
  (wrap-combiner [this combiner-fn]
    "A call to (wrap-combiner a-hadoop-job-wrapper (fn [key values context] ...)) will produce a new
    (fn [key values context] ...) that wraps the initial function to do extra stuff.

    It is acceptable for this method to do nothing and just return combiner-fn.")
  (wrap-reducer [this reducer-fn]
    "A call to (wrap-reducer a-hadoop-job-wrapper (fn [key values context] ...)) will produce a new
    (fn [key values context] ...) that wraps the initial function to do extra stuff.

    It is acceptable for this method to do nothing and just return reducer-fn."))

(defrecord StagedHadoopJob [job-name staging-location]
  HadoopJob
  (starter [this]
    (println "Creating job" job-name)
    (fn []
      (let [logger (Logger/getLogger (str job-name))]
        (doto (StagedOutputJob. (format "%s/%s" staging-location job-name) logger)
          (.setJobName job-name)
          (.setJarByClass GenericClojureJob)
          (.setMapperClass ClojureMapper)
          (.setReducerClass ClojureReducer)
          (FileInputFormat/setInputPathFilter HiddenFilePathFilter)))))
  (mapper [this] (fn [key value context] [[key value]]))
  (combiner [this] (fn [key values context] (map (partial vector key) values)))
  (reducer [this] (fn [key values context] (map (partial vector key) values))))

; Combines a HadoopJobWrapper and a HadoopJob to make something better
(deftype WrappedHadoopJob [baseJob wrapper]
  HadoopJob
  (starter [this] (wrap-starter wrapper (starter baseJob)))
  (mapper [this] (wrap-mapper wrapper (mapper baseJob)))
  (combiner [this] (wrap-combiner wrapper (combiner baseJob)))
  (reducer [this] (wrap-reducer wrapper (reducer baseJob))))

;;; ------------ Base

; A HadoopJobWrapper that does nothing.
(deftype BaseWrapper []
  HadoopJobWrapper
  (wrap-starter [this starter-fn] starter-fn)
  (wrap-mapper [this mapper-fn] mapper-fn)
  (wrap-combiner [this combiner-fn] combiner-fn)
  (wrap-reducer [this reducer-fn] reducer-fn))

(def base-wrapper (BaseWrapper.))


;;; ------------ Starter

; A HadoopJobWrapper that only wraps a starter.  starter-wrapping-fn is a (fn [starter-fn] ...) that returns a
; (fn [job] ...) that will be used as the starter-fn
(deftype StarterWrapper [starter-wrapping-fn]
  HadoopJobWrapper
  (wrap-starter [this starter-fn] (starter-wrapping-fn starter-fn))
  (wrap-mapper [this mapper-fn] mapper-fn)
  (wrap-combiner [this combiner-fn] combiner-fn)
  (wrap-reducer [this reducer-fn] reducer-fn))

(defn add-config
  "setup-fn is a (fn [job] ...) that does what it needs to do to the job object to get things to run.

  setup-fn should return the job"
  [setup-fn]
  (StarterWrapper.
    (fn [starter-fn]
      (fn [] (setup-fn (starter-fn))))))

;;; ----------- Mapper

; A HadoopJobWrapper that wraps the mapper.  mapper-wrapping-fn is a (fn [mapper-fn] ...) that returns a
; (fn [key value context] ...) that will be used as the mapping function.
(deftype MapperWrapper [mapper-wrapping-fn]
  HadoopJobWrapper
  (wrap-starter [this starter-fn] starter-fn)
  (wrap-mapper [this mapper-fn] (mapper-wrapping-fn mapper-fn))
  (wrap-combiner [this combiner-fn] combiner-fn)
  (wrap-reducer [this reducer-fn] reducer-fn))

(defn mapper-wrapper
  "This function exists to expose the constructor for MapperWrapper."
  [mapper-wrapping-fn]
  (MapperWrapper. mapper-wrapping-fn))

(defn lazy-map-mapper
  "Applies a mapping function to the output of a mapper.

  A mapping function is a (fn [key value context] ...) that returns a [[key value] ...] sequence

  map-fn is a (fn [] ...) that returns a mapping function."
  [map-fn]
  (mapper-wrapper
    (fn [mapper-fn]
      (let [real-map-fn (map-fn)]
        (fn [key value context] (mapcat (fn [[nkey nvalue]] (real-map-fn nkey nvalue context)) (mapper-fn key value context)))))))

(defmacro map-mapper
  "Applies a mapping function to the output of a reducer.

  A mapping function is a (fn [key value context] ...) that returns a [[key value] ...] sequence"
  [map-fn]
  `(lazy-map-mapper (fn [] ~map-fn)))


(defmacro filter
  "Applies a filtering function to the mapper.

  filter-fn is a (fn [value] ...) that returns true if the object should be included and false if it should be ignored."
  [filter-fn]
  `(map-mapper
    (let [actual-filter-fn# ~filter-fn]
      (fn [key# value# context#] (if (actual-filter-fn# value#) [[key# value#]] [])))))


;;; ------------ Combiner

; A HadoopJobWrapper that wraps the combiner.  combiner-wrapping-fn is a (fn [combiner-fn] ...) that returns a
; (fn [key values context] ...) that will be used as the combiner function.
(deftype CombinerWrapper [combiner-wrapping-fn]
  HadoopJobWrapper
  (wrap-starter [this starter-fn] starter-fn)
  (wrap-mapper [this mapper-fn] mapper-fn)
  (wrap-combiner [this combiner-fn] (combiner-wrapping-fn combiner-fn))
  (wrap-reducer [this reducer-fn] reducer-fn))

; A HadoopJobWrapper that wraps the combiner.  combiner-input-fn is a (fn [value keys context] ...) that returns a [key [value ...]]
; combiner-input-fn is applied to the input of the reducer first and its return value is passed on to the "actual" reducer.
; Note that this means that CombinerInputWrappers are applied in reverse order of declaration, i.e.
;
; (ComposedWrapper. (CombinerInputWrapper. (fn [value keys context] (foo value keys))
;                   (CombinerInputWrapper. (fn [value keys context] (bar value keys)))
;
; Will actually apply as (bar (foo value keys context)) instead of (foo (bar value keys context))
(deftype CombinerInputWrapper [combiner-input-fn]
  HadoopJobWrapper
  (wrap-starter [this starter-fn] starter-fn)
  (wrap-mapper [this mapper-fn] mapper-fn)
  (wrap-combiner [this combiner-fn] (fn [key values context] (let [[new-key new-values] (combiner-input-fn key values context)] (combiner-fn new-key new-values context))))
  (wrap-reducer [this reducer-fn] reducer-fn))

(defn create-combiner
  "Sets the combiner for a hadoop job.

  combine-fn is a (fn [value keys context] ...) that returns a [[key value] ...]

  combine-fn's output is intended as the output for the combiner and will not delegate to any other combine functions, so order
  is very important"
  [combine-fn]
  (CombinerWrapper. (fn [ignored-fn] combine-fn)))

; A HadoopJobWrapper that wraps the combiner.  map-fn is a (fn [value key context] ...) that returns a [[key value] ...]
; map-fn is the same as a the map-fn for MapperMapper, it is just applied to the output of the combiner rather than the output
; of the hadoop mapper.
(deftype TransformCombinerWrapper [map-fn]
  HadoopJobWrapper
  (wrap-starter [this starter-fn] starter-fn)
  (wrap-mapper [this mapper-fn] mapper-fn)
  (wrap-combiner [this combiner-fn] (fn [key values context] (mapcat (fn [[nkey nvalue]] (map-fn nkey nvalue context)) (combiner-fn key values context))))
  (wrap-reducer [this reducer-fn] reducer-fn))


;;; ------------- Reducer

; A HadoopJobWrapper that wraps the combiner.  reducer-wrapping-fn is a (fn [reducer-fn] ...) that returns a
; (fn [key values context] ...) that will be used as the reducer function.
(deftype ReducerWrapper [reducer-wrapping-fn]
  HadoopJobWrapper
  (wrap-starter [this starter-fn] starter-fn)
  (wrap-mapper [this mapper-fn] mapper-fn)
  (wrap-combiner [this combiner-fn] combiner-fn)
  (wrap-reducer [this reducer-fn] (reducer-wrapping-fn reducer-fn)))


(defn wrap-reducer-input
  "A HadoopJobWrapper that wraps the reducer.  reducer-input-fn is a (fn [value keys context] ...) that returns a [key [value ...]]
  reducer-input-fn is applied to the input of the reducer first and its return value is passed on to the \"actual\" reducer.
  Note that this means that ReducerInputWrappers are applied in reverse order of declaration, i.e.

    (compose-wrappers (wrap-reducer-input (fn [value keys context] (foo value keys))
                      (wrap-reducer-input (fn [value keys context] (bar value keys)))

  Will actually apply as (bar (foo value keys context)) instead of (foo (bar value keys context))"
  [reducer-input-fn]
  (ReducerWrapper.
    (fn [reducer-fn]
      (fn [key values context] (let [[new-key new-values] (reducer-input-fn key values context)] (reducer-fn new-key new-values context))))))

(defn create-reducer
  "Sets the reducer for a hadoop job.

  reduce-fn is a (fn [value keys context] ...) that returns a [[key value] ...]

  reduce-fn's output is intended as the output for the reducer and will not delegate to any other reduce functions, so order
  is very important"
  [reduce-fn]
  (ReducerWrapper.
    (fn [ignored-fn] reduce-fn)))

(defn lazy-map-reduce-output
  "Applies a mapping function to the output of a reducer.

  A mapping function is a (fn [key value context] ...) that returns a [[key value] ...] sequence

  map-fn is a (fn [] ...) that returns a mapping function."
  [map-fn]
  (ReducerWrapper.
    (fn [reducer-fn]
      (let [real-map-fn (map-fn)]
        (fn [key values context] (mapcat (fn [[nkey nvalue]] (real-map-fn nkey nvalue context)) (reducer-fn key values context)))))))

(defmacro map-reduce-output
  "Applies a mapping function to the output of a reducer.

  A mapping function is a (fn [key value context] ...) that returns a [[key value] ...] sequence"
  [map-fn]
  `(lazy-map-reduce-output (fn [] ~map-fn)))

(defmacro filter-reduce-output
   "Applies a filter function to the output of the reducer.

   filter-fn is a single argument function that operates only on the value"
  [filter-fn]
  `(map-reduce-output
    (let [actual-filter-fn# ~filter-fn]
      (fn [key# value# context#] (if (actual-filter-fn# value#) [[key# value#]] [])))))


; A HadoopJobWrapper that composes two HadoopJobWrappers into one.  It composes them such that the input function
; to f is the output function from g (i.e. f of g)
(deftype ComposedWrapper [f g]
  HadoopJobWrapper
  (wrap-starter [this starter-fn] (wrap-starter f (wrap-starter g starter-fn)))
  (wrap-mapper [this mapper-fn] (wrap-mapper f (wrap-mapper g mapper-fn)))
  (wrap-combiner [this combiner-fn] (wrap-combiner f (wrap-combiner g combiner-fn)))
  (wrap-reducer [this reducer-fn] (wrap-reducer f (wrap-reducer g reducer-fn))))

(defn compose-wrappers
  "A function that composes a sequence of HadoopJobWrappers such that they will apply from bottom up.

  That is (compose-wrappers foo bar baz) will apply as (foo (bar (baz incoming-fn))).  This is the same as the
  clojure.core/comp function."
  [& wrappers]
  (cond
    (= (count wrappers) 0) base-wrapper
    (= (count wrappers) 1) (first wrappers)
    (= (count wrappers) 2) (ComposedWrapper. (first wrappers) (second wrappers))
    true (ComposedWrapper. (first wrappers) (apply compose-wrappers (rest wrappers)))))

(defn select
  "Selects some subset of the input value"
  [indexes]
  (map-mapper (fn [key value context] [[key (select-keys value indexes)]])))

(defn rename
  "Renames keys in the value of an input map.

  name-pairs is a [[curr-name new-name] ...] list"
  [name-pairs]
  (map-mapper
    (fn [key value context]
      [[key
        (reduce
          (fn [old-map [curr-name new-name]]
            (let [val (get old-map curr-name)]
              (assoc (dissoc old-map curr-name) new-name val)))
          (into {} value)
          name-pairs)]])))

(defn project
  "Projects some extra columns into the value.

  projections is a [[column-name projection-fn] ...] sequence
  projection-fn is a (fn [value] ...) that returns the value to be stored under column-name"
  [projections]
  (map-mapper
    (fn [key value context]
      [[key
        (reduce
          (fn [ret-val [column-name projection-fn]]
            (assoc ret-val column-name (projection-fn ret-val)))
          (if (associative? value) value (into {} value))
          projections)]])))

(defn group-by
  "Does a group-by operation over the fields specified and projects out the projections

  - fields is a [field-name ...] sequence
  - projections is a [[name default-fn accumulator-fn] ...] sequence, where
  -- default-fn is what's used to get the data on the map side
  -- accumulator-fn is what's used to accumulate the data on the reduce side

  Note that this group-by does *not* specify an intermediate serialization scheme.  That must be defined externally."
  [fields projections]
  (compose-wrappers
    (map-mapper
      (fn [key value context]
        [[(select-keys value fields)
          (reduce conj {} (map (fn [[name default-fn acc-fn]] {name (default-fn value)}) projections))]]))
    (create-reducer
      (fn [key values context]
        [[false
          (conj (conj {} key)
            (reduce
              (fn [old-val new-val]
                (reduce conj {}
                  (map (fn [[name default-fn acc-fn]] {name (acc-fn (get old-val name) (get new-val name))}) projections)))
              values))]]))))

(defn intermediate-serialization
  "Creates a HadoopJobWrapper that sets up intermediate serialization.

  - starter-fn is a (fn [job] ...) that takes the Hadoop Job and sets the proper intermediate objects
  - mapper-fn is a (fn [key value context] ...) that maps the key and value into the proper objects for the intermediate
  data pass (i.e. serialize into BytesWritable, or Text or something like that)
  - reducer-fn is a (fn [key values context] ...) that returns a deserialized [key values] object which will be used by subsequent functions
  in the reducer.  key is just an object of some type, but values should be a sequence."
  [starter-fn mapper-fn reducer-fn]
  (compose-wrappers
    (wrap-reducer-input reducer-fn)
    (TransformCombinerWrapper. (comp vector mapper-fn))
    (CombinerInputWrapper. reducer-fn)
    (map-mapper (comp vector mapper-fn))
    (add-config starter-fn)))

(defn map-only
  "Makes the job map-only."
  []
  (add-config
    (fn [#^Job job]
      (.setNumReduceTasks job 0)
      job)))

(defn set-num-reducers
  "Makes the job map-only."
  [num]
  (add-config
    (fn [#^Job job]
      (.setNumReduceTasks job num)
      job)))

(defn use-combiner
  "Tells the job to use the combiner."
  []
  (add-config
    (fn [#^Job job]
      (.setCombinerClass job ClojureCombiner)
      job)))

(defn apply-properties-to-configuration
  "Adds all input properties to the Configuration object on the Job"
  [#^Properties properties]
  (add-config
    (fn [#^Job job]
      (set-job-config
        job
        (map (fn [key] [key (.getProperty properties key)])
          (.stringPropertyNames properties)))
      job)))

(defn text-file-input
  "Sets the input of the job to be to a text file using TextInputFormat"
  [path]
  (compose-wrappers
    (map-mapper
      (fn [#^LongWritable key #^Text value _]
        [[(.get key) (.toString value)]]))
    (add-config
      (fn [#^Job job]
        (when (nil? path) (throw (RuntimeException. (format "Input on job[%s] cannot be null." (.getJobName job)))))
        (doto job
          (.setInputFormatClass TextInputFormat)
          (FileInputFormat/addInputPaths #^String path))))))

(defn text-file-output
  "Sets the output of the job to be to a text file using TextOutputFormat (which essentially just does a .toString() on
  the key and value"
  [path]
  (add-config
    (fn [#^Job job]
      (when (nil? path) (throw (RuntimeException. (format "Output on job[%s] cannot be null." (.getJobName job)))))
      (doto job
        (.setOutputFormatClass TextOutputFormat)
        (FileOutputFormat/setOutputPath (Path. path))))))

(defmacro run
  "Sets up the jobs to run.  Takes a sequence of jobs that are run in the order specified"
  [& jobs]
  `(def ~(symbol "the-jobs")
    ~(vec jobs)))

(defmacro staged-job
  "Sets up a staged job.

  job-name is the name of the job as will appear on the JobTracker.
  staging-location is a path where the job should stage its output.
  wrappers is a sequence of wrappers that are used to define the MapReduce job.  They are applied in top-down order."
  [[job-name staging-location] & wrappers]
  `(WrappedHadoopJob.
    (StagedHadoopJob. ~job-name ~staging-location)
    (compose-wrappers ~@(reverse wrappers))))

(defn map-function
  "Creates an actual map function that uses the Context object to write things out.  This is used by ClojureMapper and
  shouldn't ever need to be used from clojure code."
  [mapper]
  (fn [key value context]
    (doseq [[out-key out-value] (mapper key value context)]
      (.write #^MapContext context out-key out-value))))

(defn reduce-function
  "Creates an actual reduce function that uses the Context object to write things out.  This is used by ClojureReducer and
  shouldn't ever need to be used from clojure code."
  [reducer]
  (fn [key value context]
    (doseq [[out-key out-value] (reducer key value context)]
      (.write #^ReduceContext context out-key out-value))))

(defn- set-job-config
  [#^Job job key-val-seq]
  (let [conf (.getConfiguration job)]
    (doseq [[key value] key-val-seq]
      (.set conf key value))))