** WORK IN PROGRESS **
# Snowplow Bigquery Lambda Loader


A `node.js` script that reads data from kinesis stream and send to google bigquery

## How to use

* Setup [snowplow with scala stream collector](https://github.com/snowplow/snowplow/wiki/Setting-up-a-Collector) and enrich
* Clone this repo
* Fill `auth.json` file with your google cloud cert
* Create a zip from the root of this repo
* Setup a AWS Lambda function with generated zip
* TODO build a step by step guide to how to setup aws lambda

## Develop
This repo have the `node_modules` directory. Is not convencional put this under source control, but into this case is needed because lambda require that the node modules builded into same architeture that into runs (node 4.16 and amazon linux). This `node_modules` are alredy build into this way.

To run this locally, make a backup of this directory, and run `npm install`.
