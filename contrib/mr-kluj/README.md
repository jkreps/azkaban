# Mr.Kluj

Mr.Kluj is a kludge of a MapReduce library written in Clojure.  It is intended to be used primarily for production MapReduce workflows but allow for scripted interaction to make it easier to build the workflows.  The why of this library basically lies in issues I had with the current methods of writing Hadoop MapReduce jobs:

* **Hadoop Java API**
    1. Verbose
    2. Multi-job workflows are _very_ difficult to maintain as it is difficult to figure out exactly what data is where.
* **Hive**
    1. SQL queries are concise and work well for pulling data out of structured tables, but whenever I look at a workflow of SQL queries, I always have to start over at the beginning to figure out what that table I'm pulling data from actually has in it.
    2. Hides the key/value algebra from the developer
* **Pig**
    1. Hides the key/value algebra from the developer
    2. UDFs are a pain: simple ones are simple, but the minute a UDF breaks, it requires a week-long trudge through Pig code to figure out why it broke
* **Cascading**
    1. Requires compilation of Java code in order for things to work
    2. Does not support combiners

While I can't claim that this solves all of the problems I have with the other options, I like to think it's a good start toward something that is more transparent when it breaks and allows the developer to choose the level of abstraction that they want to work at.

**How it works**

Mr.Kluj is built on the idea that there are four parts to every map reduce job:

    1. Starter
    2. Mapper
    3. Combiner
    4. Reducer

The Starter is the client-side piece that sets up the job.  In Hadoop MapReduce, this is what instantiates a Configuration object and sets all of the various parameters for the MR job.

The Mapper is the equivalent of a mapping function from the functional programming world except that it emits a `(key, value)` pair.

The Reducer is the equivalent of a reduce function from the functional programming world except that it takes in a `key` and then reduces over a sequence of `value`s.

The Combiner is just a Reducer with some additional constraints on the output it accepts and produces.

Note: This library assumes knowledge of Hadoop MapReduce, so if the above explanation of the four parts doesn't make sense, I would recommend [reading about Hadoop](http://hadoop.apache.org/) before trying to dig into the internals of Mr.Kluj

These four parts to a MapReduce job basically enumerate a set of four functions that need to be defined in order to run a MapReduce job.  Thus, all that's needed is a way to define these four functions and a way to pass those definitions across the wire to their respective TaskTrackers.  Clojure provides for both of these mechanisms because it handles scripted interaction.  So, the script itself should be the definition of the four functions and can be utilized as the mechanism of shipping these functions across the wire.

To get into specifics, the clojure defines a protocol:

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

As stated in the document strings, each of these essentially returns a lambda that does whatever is needed for that stage of processing: 

* `starter` is a no argument function that returns a Job object (new APIs)
* `mapper` is a three argument function that will be used for the map() method on the Mapper class
* `combiner` is a three argument function that will be used for the reduce() method on the Reducer class
* `reducer` is a three argument function that will be used for the reduce() method on the Reducer class

This is the basic building block of Mr.Kluj.  If a script generates one of these (actually, it needs to generate a sequence of them), then that script can be used on the client side by interpretting the script and then asking for the `starter`, which it invokes to get a Job object.  The script can then be sent along to the Mapper/Combiner/Reducer and they can load it up and then ask for the mapper/combiner/reducer functions respectively, which can then be used to do the actual mapping and reducing.

Mr.Kluj provides classes `[com.linkedin.mr_kluj GenericClojureJob ClojureMapper ClojureCombiner ClojureReducer]` which are set up to work with scripts, push them into Configuration and pull them out of Configuration to make sure that all parts of the MapReduce job have what they need.

So, that resolves the question of how to define a MapReduce job: just create an instance of the protocol HadoopJob.  This is the lowest level interaction made possible by Mr.Kluj.  It doesn't enforce any constraints on how you write your code (except that it be clojure) and just gets you interfacing with Hadoop. 

Defining protocols isn't particularly pretty, though, and it doesn't lend itself to creating individual pieces of functionality that can be reused across MapReduce jobs.  Thus, Mr.Kluj provides another abstraction layer that attempts to make it easier to take normal clojure code and make it reusable in a MapReduce job.  The protocol for this is

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
     
This is a Wrapper protocol as hinted at by the name.  More specifically, a concrete instance of this protocol defines a set of transformations on the `starter`, `mapper`, `combiner`, and `reducer`.  These transformations are generally applied via function composition (i.e. the result of the `starter` for example is passed on to another function before actually being returned).  There are then a couple of base functions defined that allow for ease in creating Wrapper instances:

* `add-config`: takes a (fn [job] ...) as an argument and applies it to the job object.
* `map-mapper`: takes a (fn [key value context] ...) as an argument and applies that as a mapping function
* `create-combiner`: takes a (fn [key values context] ...) as an argument and uses that function as the combiner
* `create-reducer`: takes a (fn [key values context] ...) as an argument and uses that function as the reducer

These are all implemented as `HadoopJobWrapper`s that each adjust one specific part of the job functions.  They allow the definition of a MapReduce job to look something like

    (require '[com.linkedin.mk-kluj.job :as job])
    
    (job/run
     (job/staged-job ["job-name" "/tmp/staging/path"]
		     (job/add-config
		      (fn [job]
			(.setNumReducers job 10)))
		     (job/map-mapper
		      (fn [key value context]
			[[(Text. key) (IntWritable. (* value value))]]))
		     (job/create-reducer
		      (fn [key values context]
			[[(.toString key) (reduce + (map (memfn get) values))]]))))

Which isn't a particularly useful job (and lacks specification of input and output, so it won't actually work...), but ostensibly it 

1. Sets the config to have 10 reducers.  
2. Specifies a mapper function that squares the value.
3. Specifies a reducer function that sums up all the values under a given key.

So, this would result in the sum of squares for each given key.

This is all fine and good, but for those of you who know SQL or other data aggregation languages, this is just a group by with an aggregation function.  In fact, it turns out that the designers of SQL actually knew something of what they were talking about when they defined things like "group by" and "join".  These are operations that come up constantly in data workflows.  Also, there's a good amount of ugliness injected into the code because in the MapReduce world you have to deal with serializing your objects between the mapper and the combiner/reducer.  With just the abstraction layer defined above, it's easy to foresee a lot of duplicate code getting this stuff going.

However, the abstraction layer also provides the building blocks required to make higher level primitives around specific intermediate data serialization schemes.  These are also provided with Mr.Kluj using voldemort serialization as a serialization library for intermediate data.  Voldemort serialization specifically provides the following functions:

* `make-schema`
* `get-schema`
* `voldemort-storage-input`
* `voldemort-storage-output`
* `voldemort-intermediate-data`
* `group-by`
* `group-by-no-combine`
* `join`

These functions are all documented in the source code, but they allow for higher level definitions of jobs like the following.

    (require '[com.linkedin.mr-kluj.voldemort-serialization :as vold])
    
    (job/run
     (job/staged-job ["job-name" "/tmp/staging/path"]
		     (vold/voldemort-storage-input "/voldemort/serialized/input/path")
		     (job/apply-properties-to-configuration user/*props*)
		     (job/filter #(not (= (get % "type") "spammer")))
		     (vold/group-by
		      [["profile_id" "'int64'"]]
		      [["num_friends" (fn [val] 1) + "'int32'"]
		       ["friends_names_mashed" #(get % "fullname") str "'string'"]])
		     (vold/voldemort-storage-output
		      "/some/output/path"
		      "'boolean'"
		      "{ 'profile_id':'int64', 'num_friends':'int32', 'friends_names_mashed':'string'}")))

Going through this job, it starts out by creating a staged-job.  A staged-job is a job that stages its output to a temporary location before blowing away whatever is in its final destination and replacing it with what it just produced.  It takes a job name and a staging path (a base directory for it to use when writing its output) and then a list of HadoopJobWrappers:

    (vold/voldemort-storage-input "/voldemort/serialized/input/path")

Is just an `add-config` which sets up the job to take input from a voldemort serialized file at the given path.

     (job/apply-properties-to-configuration user/*props*)

Is another `add-config` which applies all the properties from the user/*props* object.  The user/*props* object is one of two global variables defined by Mr.Kluj.  It is a java.util.Properties object that is useful as a method of parameterizing your hadoop jobs.  The other global variable defined is user/*context* which provides access to the context object.

	(job/filter #(not (= (get % "type") "spammer")))

Is a `map-mapper` that applies a filter to the data.  It is given a unary function that returns a true/false.  This particular function pulls the "type" field out of the value and checks if it is the word "spammer", ostensibly filtering out spammers such that they aren't a part of the calculation.

This also exemplifies a two important assumptions that are made in this abstraction layer.  The assumptions are as follows:

1. Data is stored entirely in the value, the key can essentially be ignored.  This means that functions like filter only operate on the _value_ of the `(key,value)` pair.  The point where the key is important is when dealing with intermediate data.  That boundary is dealt with for you by the `group-by` and `join` functions, which take as input a key which is ignored and emit a key of boolean false as output.
2. Data stored in a value is a java.util.Map.  Maps work as a decent abstraction of a row of data (if you want a specific column, that's the same as getting that column name from the map).

        (vold/group-by
          [["profile_id" "'int64'"]]
          [["num_friends" (fn [val] 1) + "'int32'"]
           ["friends_names_mashed" #(get % "fullname") str "'string'"]])

Is a combination of many wrappers (`map-mapper, add-config, create-combiner, create-reducer, map-reduce-output`) that all combine to perform a group-by operation, grouping by the "column" `profile_id` which is of type `int64` (or `long` in Java terms).  For each `profile_id` it computes two projection/aggregations: `num_friends` and `friends_names_mashed`.  `Num_friends` emits a default value of 1 for each row in the mapper and then is aggregated with the `+` operation.  `Friends_names_mashed` emits the `fullname` of the profile for each row and then aggregates them together with the `str` function (this last aggregation really isn't that useful as the order in which `str` will be called is not guaranteed, but oh well, it's an example).  Note that they both specify the type of the output as well.

    (vold/voldemort-storage-output
      "/some/output/path"
      "'boolean'"
      "{ 'profile_id':'int64', 'num_friends':'int32', 'friends_names_mashed':'string'}")

Is an `add-config` that specifies the output location and serialization scheme for the job.  It will generate output into a file at "/some/output/path" with the given schemas (key first, value second).

In this way, the wrappers can be combined to produce high-level, reusable functionality, while still providing low-level access to what is happening in the MapReduce world, if needed.

That completes the long story of how to use Mr.Kluj.  Hopefully that explains what it is, why it is and gives some insight into whether it will be helpful for you as well.  This writeup did not cover all of the functions provided, so please look at the API docs for a complete list of what's available.

## Usage

First thing to do after checking out the repository is the first thing you do with every lein project:

     lein deps

The first time you do this, there will be some missing dependencies.  The jars are included in the `jars-to-install` directory.  Just copy the maven installation command-line from the error messages and adjust them to point to the right files for installation.  That is, you will probably get errors that look like

    1) org.apache.hadoop:hadoop:jar:0.20.3-dev-core

     Try downloading the file manually from the project website.

     Then, install it using the command: 
        mvn install:install-file -DgroupId=org.apache.hadoop -DartifactId=hadoop -Dversion=0.20.3-dev-core -Dpackaging=jar -Dfile=/path/to/file

     Alternatively, if you host your own repository you can deploy the file there: 
        mvn deploy:deploy-file -DgroupId=org.apache.hadoop -DartifactId=hadoop -Dversion=0.20.3-dev-core -Dpackaging=jar -Dfile=/path/to/file -Durl=[url] -DrepositoryId=[id]

     Path to dependency: 
 	1) org.apache.maven:super-pom:jar:2.0
 	2) org.apache.hadoop:hadoop:jar:0.20.3-dev-core

the correct response to this is

    mvn install:install-file -DgroupId=org.apache.hadoop -DartifactId=hadoop -Dversion=0.20.3-dev-core -Dpackaging=jar -Dfile=jars-for-installation/hadoop-0.20.3-dev-core.jar

After that all you should have to do to build and run is

      ant
      java -jar dist/jar-with-deps/mr-kluj-1.0.0.jar
 
This will display the "help" text which is partially helpful

     Usage: <java-command> clj-script-file key value key value

* `<java-command>` represents the java command, e.g. `java -jar dist/jar-with-deps/mr-kluj-1.0.0.jar` or `java -cp dist/jar-with-deps/mr-kluj-1.0.0.jar com.linkedin.mr_kluj.GenericClojureJob` and all the `-D` properties you want.
* `clj-script-file` is a clojure script that has your job in it.
* `key value key value` is a set of alternating key-value pairs that are passed into the script via the `user/*props*` global variable

Note: the build of this currently uses a weird mix of lein and ant.  If anyone out there is well with the ways of lein, I've been unable to get it to generate an uberjar because it doesn't seem to want to compile the java classes included.  Any help is appreciated.

## Installation

Generally covered in the Usage section.

## License

Copyright 2010 LinkedIn, Inc
 
Licensed under the Apache License, Version 2.0 (the "License"); you may not
use this file except in compliance with the License. You may obtain a copy of
the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
License for the specific language governing permissions and limitations under
the License.

