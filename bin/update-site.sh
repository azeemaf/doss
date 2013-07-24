#!/bin/bash

set -e -u

git checkout master
mvn clean test site javadoc:javadoc
git checkout gh-pages
rm -R apidocs coverage
cp -a target/site/apidocs apidocs
cp -a target/site/jacoco coverage

# github pages doesn't like dotfiles
mv coverage/{.,}sessions.html
mv coverage/{.,}resources
find coverage -type f -exec sed -i -e 's/\.resources/resources/g' -e 's/\.sessions.html/sessions.html/g' {} +

git add --all apidocs/ coverage/
git commit -m"Updated generated documentation"
git push origin gh-pages
