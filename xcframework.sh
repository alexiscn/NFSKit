#!/bin/sh -xe

Help()
{
   # Display Help
   echo "build libnfs xcframework."
   echo "usage: xcframework.sh [-h]"
   echo "options:"
   echo "l     : Optional, libraries(libnfs)."
   echo "h     : Optional, Print this Help."
   echo
}

LIBRARIES="nfs"
PLATFORMS="macos ios tvos isimulator tvsimulator maccatalyst"
ROOT="$(pwd)"
Framework="$ROOT/Framework"

while getopts ":hp:a:l:d" OPTION; do
    case $OPTION in
    h)
        Help
        exit
        ;;
    l)
        LIBRARIES=$(echo "$OPTARG" | awk '{print tolower($0)}')
        ;;
    ?)
        echo "Invalid option"
        exit 1
        ;;
    esac
done

for LIBRARY in $LIBRARIES; do
    arguments=""
    LIBRARY_NAME="lib$LIBRARY"
    for PLATFORM in $PLATFORMS; do
        ARCHS="x86_64 arm64"
        if [[ "$PLATFORM" = "ios" || "$PLATFORM" = "tvos" ]]; then
            ARCHS="arm64"
        fi
        SCRATCH="$ROOT/build/scratch-$PLATFORM"
        mkdir -p $SCRATCH/$LIBRARY_NAME.universal
        lipo_arguments=""
        for ARCH in $ARCHS; do
            if [[ ! -f "$SCRATCH/$ARCH/lib/$LIBRARY_NAME.a" ]]; then
                continue
            fi
            lipo_arguments="$lipo_arguments $SCRATCH/$ARCH/lib/$LIBRARY_NAME.a"
            header_dir=""
            if [[ -d "$SCRATCH/$ARCH/include/$LIBRARY" ]]; then
                header_dir="$SCRATCH/$ARCH/include/$LIBRARY"
            elif [[ -d "$SCRATCH/$ARCH/include/nfsc" ]]; then
                header_dir="$SCRATCH/$ARCH/include/nfsc"
            fi
            mkdir -p $SCRATCH/$LIBRARY_NAME.universal/include
            if [[ $header_dir != "" ]]; then
                cp -R $header_dir $SCRATCH/$LIBRARY_NAME.universal/include/
            fi
        done
        if [[ $lipo_arguments = "" ]]; then
            continue
        fi
        lipo -create $lipo_arguments -output $SCRATCH/$LIBRARY_NAME.universal/$LIBRARY_NAME.a
        arguments="$arguments -library $SCRATCH/$LIBRARY_NAME.universal/$LIBRARY_NAME.a -headers $SCRATCH/$LIBRARY_NAME.universal/include"
    done
    
    if [[ $arguments = "" ]]; then
        continue
    fi
    rm -fr $Framework/Lib$LIBRARY.xcframework
    xcodebuild -create-xcframework $arguments -output $Framework/Lib$LIBRARY.xcframework

    #FULL_INFO_PLIST_PATH=$Framework"/"$LIBRARY".xcframework/Info.plist"
    #/usr/libexec/PlistBuddy -c "Add :MinimumOSVersion string 13.0" "$FULL_INFO_PLIST_PATH"
done
