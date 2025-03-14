# pixels-retina Visibility Native Methods
Compiled here to generate shared libraries, handed over to java to load and call.

## How to compile
The cpp library was compiled when you compiled pixels-retina. If you need to compile it manually, you can follow the steps below:
```bash
mkdir -p build
cd build
cmake ..
make
```

You can get `libpixels-retina.so` after compilation, and you can find this file in the `$PIXELS_HOME/lib` path.

## How to test

```bash
./tile_visibility_tests
./rg_visibility_tests
```