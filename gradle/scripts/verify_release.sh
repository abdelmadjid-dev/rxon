#!/bin/sh

# Get version from rxon/build.gradle.kts
VERSION=$(grep "version =" rxon/build.gradle.kts | sed -E 's/version = "(.*)"/\1/')

if [ -z "$VERSION" ]; then
    echo "ERROR: Could not determine version from rxon/build.gradle.kts"
    exit 1
fi

echo "Verifying release integrity for version: $VERSION"

# 1. Check Changelog
CHANGELOG="changelogs/v$VERSION.md"
if [ ! -f "$CHANGELOG" ]; then
    echo "ERROR: Missing changelog for version $VERSION at $CHANGELOG"
    exit 1
fi
echo "✓ Changelog found: $CHANGELOG"

# 2. Check Migration Guide
MIGRATION=$(ls migrations/* | grep "$VERSION")
if [ -z "$MIGRATION" ]; then
    echo "ERROR: No migration guide found for version $VERSION in migrations/"
    exit 1
fi
echo "✓ Migration guide found: $MIGRATION"

# 3. Check README.md
README="README.md"
INSTALL_SNIPPET="implementation(\"com.benaether:rxon:$VERSION\")"
if ! grep -Fq "$INSTALL_SNIPPET" "$README"; then
    echo "ERROR: README.md installation snippet is not updated to version $VERSION."
    echo "Expected: $INSTALL_SNIPPET"
    exit 1
fi
echo "✓ README.md installation snippet is correct."

TABLE_ENTRY="v$VERSION"
if ! grep -Fq "$TABLE_ENTRY" "$README"; then
    echo "ERROR: README.md changelog table is missing an entry for $VERSION."
    exit 1
fi
echo "✓ README.md changelog table contains $VERSION."

echo "✓ Release verification passed."
exit 0
