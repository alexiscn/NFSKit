#!/bin/sh

cd ..
rm -rf "libnfs"
mkdir  "libnfs"
mkdir  "libnfs/include"
mkdir  "libnfs/lib"
mkdir  "libnfs/nfs"
mkdir  "libnfs/nfs4"
PACKAGE_DIRECTORY=`pwd`
export LIB_OUTPUT="${PACKAGE_DIRECTORY}/libnfs/lib"
cd buildtools

brew update
for pkg in cmake automake autoconf libtool; do
    if brew list -1 | grep -q "^${pkg}\$"; then
        echo "Updating ${pkg}."
        brew upgrade $pkg &> /dev/null
    else
        echo "Installing ${pkg}."
        brew install $pkg > /dev/null
    fi
done

if [ ! -d libnfs ]; then
    git clone https://github.com/sahlberg/libnfs
    cd libnfs
    echo "Bootstrapping..."
    ./bootstrap &> /dev/null
else
    cd libnfs
fi

export USECLANG=1
export CFLAGS="-fembed-bitcode -Wno-everything -DHAVE_GETPWNAM=1 -DHAVE_SOCKADDR_LEN=1 -DHAVE_SOCKADDR_STORAGE=1 -DHAVE_TALLOC_TEVENT=1"
export CPPFLAGS="-I${PACKAGE_DIRECTORY}/buildtools/include"
export LDFLAGS="-L${LIB_OUTPUT}"

echo "Making libnfs static libararies"

FRPARAM="--disable-werror"

echo "  Build iOS"
export OS=ios
export MINSDKVERSION=9.0
../autoframework libnfs $FRPARAM > /dev/null
echo "  Build macOS"
export OS=macos
export MINSDKVERSION=10.11
../autoframework libnfs $FRPARAM > /dev/null
echo "  Build tvOS"
export OS=tvos
export MINSDKVERSION=9.0
../autoframework libnfs $FRPARAM > /dev/null
cd ..

echo  "Copying additional headers"
cp    "libnfs/include/libnfs-private.h" "${PACKAGE_DIRECTORY}/libnfs/include/"
cp    "libnfs/nfs/libnfs-raw-nfs.h" "${PACKAGE_DIRECTORY}/libnfs/nfs/"
cp    "libnfs/nfs4/libnfs-raw-nfs4.h" "${PACKAGE_DIRECTORY}/libnfs/nfs4/"
cp    "module.modulemap"                  "${PACKAGE_DIRECTORY}/libnfs/include/"

rm -rf libnfs
rm -rf include
rm -rf lib
