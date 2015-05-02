KT4J 
=====

KT4J is a simple implementation of [Kyoto Tycoon](http://fallabs.com/kyototycoon/) client for Java.

[![Build Status](https://travis-ci.org/kumai/kt4j.png?branch=master)](https://travis-ci.org/kumai/kt4j)

## Requirements

The minimum requirements to run the application using KT4J are only three:

* JDK 6 or above
* the latest version of KT4J
* [Netty](http://netty.io/) (netty-3.3.x.Final.jar)

## Usage

1. Add **kt4j-x.y.z.jar** and **netty-3.3.x.Final.jar** to the application's CLASSPATH.
2. Create _kt4j.binary.KyotoTycoonBinaryClient_ and call start() of the instance in your application.
3. Call various Kyoto Tycoon's procedures. (see _kt4j.KyotoTycoonClient_) 

Example:

    import kt4j.binary.KyotoTycoonBinaryClient;
    
    public class Bootstrap {
      public static void main(String[] args) {
        KyotoTycoonBinaryClient client = new KyotoTycoonBinaryClient("kyoto_tycoon_host", 1978);
        client.start();
            
        client.set("KEY", "VALUE");
        String v = client.get("KEY");
        System.out.println(v);  // => "VALUE"
            
        client.stop();
      }
    }

## Building

KT4J uses [Gradle](http://www.gradle.org) to build. KT4J includes [Gradle Wrapper](http://www.gradle.org/docs/current/userguide/gradle_wrapper.html).

    $ cd kt4j
    $ ./gradlew jar
