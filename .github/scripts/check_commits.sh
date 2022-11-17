#! /bin/bash

#Declare a string array
FilesArray=("gradle\databases.gradle"  "hibernate-core\hibernate-core.gradle"  "gradle\java-module.gradle"  "databases\pgsql\matrix.gradle"  "settings.gradle")

echo "Print every file's commit ID in new line"
for val in ${FilesArray[*]}; do
  commitID=$(git rev-list -1 HEAD "$val")
  echo "$commitID"
  echo "$HIBERNATE_COMMIT_ID"
  if [ "$commitID" = "$HIBERNATE_COMMIT_ID" ]; then
    echo "commitIDs are equal'"
  fi
done
