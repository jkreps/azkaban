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

(ns com.linkedin.mr-kluj.utils)

(defn make-select-all-fn
  [fields]
  "Creates a (fn [map] ...) that selects all keys as in select-keys from the map.

   fields is a seq of either srings or [name default] vectors.
     a string value will simply select the field if it exists and insert nil if it does not exist.
     a vector value specifies the name of the field as well as the default value."
  (let [selection-fns
        (map
          (fn [field-spec]
            (if (string? field-spec)
              (fn [in-map] [field-spec (get in-map field-spec nil)])
              (let [[field-name default-value] field-spec]
                (fn [in-map] [field-name (get in-map field-name default-value)]))))
          fields)]
    (fn [in-map] (into {} (map #(% in-map) selection-fns)))))

(defn- aselect
  "Selects indexes out of an array.

   indexes is a sequence of indexes (0, 1, 2, etc.)
   returns a vector of the indexes pulled out of the array"
  [indexes]
  (fn [^"[Ljava.lang.String;" arg]
    (vec (map (fn [index] (when (< index (alength arg)) (aget arg index))) indexes))))

(defn cut
  "Creates a function that takes a String as input, splits the string according to the 
   delimiter and then selects the specified indexes.  Similar to the UNIX cut command,
   except indexes start from 0:

   cut -f 1,2 -> (cut [0 1])
   cut -d ',' -f 6,8 -> (cut \",\" [5 7])

   indexes is a list of integer indexes.
   delimiter is the delimiter of the split (defaults to tab)"
  ([indexes] 
     (cut "\t" indexes))
  ([delimiter indexes]
     (let [select-fn (aselect indexes)]
       (fn [arg]
	 (select-fn (.split ^String arg delimiter))))))

(defn assoc-fn
  "Applies a function to multiple indices of a vector, replacing those indices with the
   output of the function.

   ((assoc-fn #(* % 2) 1) [0 5 10 15 20 25 30]) -> [0 10 10 15 20 25 30]
   ((assoc-fn #(* % 2) [1 2 5]) [0 5 10 15 20 25 30]) -> [0 10 20 15 20 50 30]
   
   the-fn is the function to apply
   index is either a single index or a collection of indexes that specify what should be replaced"
  [the-fn index]
  (if (coll? index)
    (apply comp (map (partial assoc-fn the-fn) index))
    (fn [arg] (assoc arg index (the-fn (arg index))))))

(defn uniq
  "Returns the unique elements of a sequence.  The comparison is done based on the equality of the
   return value of key-fn.  This operates very similarly to the UNIX uniq command in that for a true
   unique set, it requires things be pre-sorted.

   (uniq identity [1 1 1 1 2 2 2 2 1 2 2 3 3 3 3]) -> [1 2 1 2 3]

   key-fn is a function that returns a key for testing equality
   sequence is a sequence of things to produce the unique values of"
  ([key-fn sequence] (let [first-val (first sequence)] (uniq (key-fn first-val) first-val key-fn (rest sequence))))
  ([key value key-fn sequence]
     (if (empty? sequence)
       [key]
       (let [first-val (first sequence)
	     first-key (key-fn first-val)]
	 (if (= first-key key)
	   (recur key value key-fn (rest sequence))
	   (lazy-seq (cons value (uniq first-key first-val key-fn (rest sequence)))))))))

(defn with-count
  "Wraps the sequence such that it will print out status text after each mod-operand elements
   have been read.  This is basically useful when processing large files and you want status
   messages to ensure that stuff is actually happening.  This is lazy, so it will not traverse
   any elements on its own, a reduce/doseq/etc. call is required to make anything actually 
   happen."
  ([mod-operand coll] (with-count mod-operand 0 (java.lang.System/currentTimeMillis) coll))
  ([mod-operand count startMillis coll]
     (if (empty? coll) 
       nil
       (let [mod-eq-0 (= (mod count mod-operand) 0)
	     workingMillis (if mod-eq-0 (java.lang.System/currentTimeMillis) startMillis)]
	 (do (when mod-eq-0  (println (format "%s millis, %sth line" (- workingMillis startMillis) count)))
	     (lazy-seq
	      (cons (first coll)
		    (with-count mod-operand (inc count) workingMillis (rest coll)))))))))
