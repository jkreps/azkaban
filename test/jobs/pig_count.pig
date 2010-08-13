REGISTER lib/linkedin-pig-0.7.jar
REGISTER ../../lib/google-collect-1.0-rc2.jar
REGISTER ../../lib/voldemort-0.70.1.jar
REGISTER ../../lib/joda-time-1.6.jar

data = LOAD 'wordcount.input' ; 
B = GROUP data ALL;
sum = FOREACH B GENERATE COUNT(data) ;

DUMP sum;

