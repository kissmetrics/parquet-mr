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

REPOSITORY_URL=https://maven.kissmetrics.com/nexus/content/repositories/releases/
REPOSITORY_ID=km-nexus
GROUP_ID=com.kissmetrics
VERSION=1.7.0-KM-1

for pom in `find . -name pom.xml -mindepth 2`; do
    dir=`dirname $pom`
    basename=`basename $dir`
    file=$dir/target/$basename*-$VERSION.jar
    if [ -e $file ]; then
        file=`ls $file`
        artifact_id=`basename $file | sed s/-1.7.0-KM-1.jar$//`
        mvn deploy:deploy-file -Durl=$REPOSITORY_URL \
            -DrepositoryId=$REPOSITORY_ID \
            -Dfile=$file \
            -DgroupId=$GROUP_ID \
            -DartifactId=$artifact_id \
            -Dversion=$VERSION \
            -Dpackaging=jar
    fi
done
