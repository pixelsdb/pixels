# DeleteTracker Native Methods
Compiled here to generate shared libraries, handed over to java to load and call.

## How to compile
```bash
mkdir -p build
cd build
cmake ..
make
```

You can get `libDeleteTrackerNative.so` after compilation, and you can find this file in the `$PIXELS_HOME/lib` path.

## How to test

```bash
./visibility_tests
./retina_tests
```
