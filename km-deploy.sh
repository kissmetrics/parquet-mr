#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# TODO: have to put the header here so 'rat' (whatever that is) doesn't complain ... :/

mvn deploy:deploy-file -Durl=https://maven.kissmetrics.com/nexus/content/repositories/releases/ -Dfile=./parquet-hadoop/target/parquet-hadoop-km-1.7.0.jar -DpomFile=./parquet-hadoop/pom.xml -DrepositoryId=km-nexus

mvn deploy:deploy-file -Durl=https://maven.kissmetrics.com/nexus/content/repositories/releases/ -Dfile=./parquet-avro/target/parquet-avro-km-1.7.0.jar -DpomFile=./parquet-avro/pom.xml -DrepositoryId=km-nexus
