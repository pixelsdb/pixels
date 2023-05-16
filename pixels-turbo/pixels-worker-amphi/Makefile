.PHONY: clean main

all: main

clean:
	rm -rf build

main:
	mkdir -p build
	cd build && cmake .. && make
	build/pixels_worker_amphi
