// swift-tools-version:5.9
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let package = Package(
    name: "NFSKit",
    products: [
        // Products define the executables and libraries a package produces, and make them visible to other packages.
        .library(
            name: "NFSKit",
            targets: ["NFSKit"])
    ],
    targets: [
        // Targets are the basic building blocks of a package. A target can define a module or a test suite.
        // Targets can depend on other targets in this package, and on products in packages this package depends on.
        .target(
            name: "NFSKit",
            dependencies: ["nfs"]),
        .testTarget(
            name: "NFSKitTests",
            dependencies: ["NFSKit"]),
        .target(
            name: "nfs",
            dependencies: ["Libnfs"],
            cSettings: [
                .define("HAVE_CONFIG_H", to: "1"),
                .define("_U_", to: "__attribute__((unused))"),
                .define("HAVE_GETPWNAM", to: "1"),
                .define("HAVE_SOCKADDR_LEN", to: "1"),
                .define("HAVE_SOCKADDR_STORAGE", to: "1"),
                .define("HAVE_TALLOC_TEVENT", to: "1")
            ]),
        .binaryTarget(
            name: "Libnfs",
            path: "Framework/Libnfs.xcframework")
    ])
