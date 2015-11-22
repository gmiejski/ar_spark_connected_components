#!/usr/bin/env bash

rm connected-components.jar
sbt package
cp target/scala-2.10/sparkpagerank_2.10-1.0.jar connected-components.jar

scp *.sh connected-components.jar plggrzmiejski@zeus.cyfronet.pl:/people/plggrzmiejski/lab2
