Azkaban jars can be deployed to a maven repository. The maven build tasks use the 
property release.version to determine the release name that is built/deployed.

By default, it is given the value curr.release.snapshot found in the build properties 
file, but it can be overridden with -Drelease.version=xxxxxxx  specified on the command line.

So, for example: 

       ant jars -Drelease.version=1.0-SNAPSHOT

builds azkaban-common and azkaban jars with version 1.0-SNAPSHOT.

Likewise, there are properties for the maven repostory and repository id to use:

* mvn.repository.id.snapshot
* mvn.repository.id.release
* mvn.repository.location.snapshot
* mvn.repository.location.release

The mvn task will choose the appropriate setting based upon whether you use
mvn-deploysnapshot or mvn-deployrelease.  Also, these can be overridden on the
commandline for a particular build by specifying -Dmvn.repository.id and/or
-Dmvn.repository.location.
   
Note that this build uses antlib in order to execute the maven parts. The plugin is in the
antlib directory, but you must add an extra -lib directive to the ant call, like:

      ant -lib antlib xxxxxxxx
      
For Maven deployment: 

      ant -lib antlib mvn-deploysnapshot
      ant -lib antlib mvn-deployrelease
      
For Maven local installation:

      ant -lib antlib mvn-installsnapshot
      ant -lib antlib mvn-installrelease
