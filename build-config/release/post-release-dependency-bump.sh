IFS="="
while read -r property value
do
    if [[ $property = "ballerinaLangVersion" ]] && [[ $value = *Preview* ]]
      then
        oldVersion=$(echo $value | cut -d'w' -f 2)
        newVersion=`expr $oldVersion + 1`
        version=$(echo $(echo $value | cut -d'w' -f 1)w$newVersion-SNAPSHOT)
        sed -i "s/ballerinaLangVersion=\(.*\)/ballerinaLangVersion=$version/g" gradle.properties
    fi
    if [[ $property = stdlib* ]]
      then

        oldVersion=$(echo $value | cut -d'.' -f 3)
        newVersion=`expr $oldVersion + 1`
        version=$(echo $(echo $value | cut -d'.' -f 1).$(echo $value | cut -d'.' -f 2).$newVersion-SNAPSHOT)
        sed -i "s/$property=\(.*\)/$property=$version/g" gradle.properties
    fi
done < gradle.properties