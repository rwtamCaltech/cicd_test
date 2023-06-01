VERSION=$(echo "$MSG" | awk -Fâ†’ '{print $2}')
echo "---------------------------------------------------"
echo "Releasing version ${VERSION} ..."
echo "---------------------------------------------------"
echo
echo
git checkout main
# git checkout build
# git merge master
git tag -a ${VERSION} -m "Releasing version ${VERSION}" #make sure tag shows up in gitlab
# git push origin ${VERSION}
echo "Pushing build to origin ..."
# git push --tags origin build
git push --tags origin main
# git checkout master
echo "Pushing main to origin ..."
git push origin main
