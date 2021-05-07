#!/bin/bash

array_contains () {
    local seeking=$1; shift
    local in=1
    for element; do
        if [[ $element == "$seeking" ]]; then
            in=0
            break
        fi
    done
    return $in
}

git fetch origin master

if [[ "${BUILDKITE_BRANCH}" == "master" ]]; then
  changes=$(git diff "${BUILDKITE_COMMIT}"^ "${BUILDKITE_COMMIT}" --name-only)
else
  changes=$(git diff origin/master...HEAD --name-only)
fi


root_folders=()
echo "changed items = ${changes}"
for change in ${changes}
do
  root=$(echo "${change}" | cut -d "/" -f1)
  array_contains "${root}" "${root_folders[@]}" || root_folders+=( "${root}" )
done

# do we force a complete build (on branch only)
shopt -s nocasematch

force=false
if [[ "${BUILDKITE_MESSAGE}" =~ \[build\] ]]; then
    force=true
fi

shopt -u nocasematch

printf '%s\n' "${root_folders[@]}" | jq -R . | jq -s '. | {folders:., force: '${force}'}' > changed_folders.json
