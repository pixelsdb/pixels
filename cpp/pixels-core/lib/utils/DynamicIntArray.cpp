//
// Created by whz on 1/13/25.
//
#include "utils/DynamicIntArray.h"


DynamicIntArray::DynamicIntArray() : DynamicIntArray(DEFAULT_CHUNKSIZE) {}


DynamicIntArray::DynamicIntArray(int chunkSize) {
    this->chunkSize = chunkSize;
    data = new int*[INIT_CHUNKS];
    length = 0;
    initializedChunks = 0;
}


int DynamicIntArray::get(int index) {
    if (index >= length) {
        throw std::out_of_range("Index " + std::to_string(index) + " is outside of valid range.");
    }
    int i = index / chunkSize;
    int j = index % chunkSize;
    return data[i][j];
}

void DynamicIntArray::set(int index, int value) {
    int i = index / chunkSize;
    int j = index % chunkSize;
    grow(i);
    if (index >= length) {
        length = index + 1;
    }
    data[i][j] = value;
}

void DynamicIntArray::increment(int index, int value) {
    int i = index / chunkSize;
    int j = index % chunkSize;
    grow(i);
    if (index >= length) {
        length = index + 1;
    }
    data[i][j] += value;
}

void DynamicIntArray::add(int value) {
    int i = length / chunkSize;
    int j = length % chunkSize;
    grow(i);
    data[i][j] = value;
    length += 1;
}


int DynamicIntArray::size() {
    return length;
}


void DynamicIntArray::clear() {
    length = 0;
    for (int i = 0; i < initializedChunks; ++i) {
        delete[] data[i];
        data[i] = nullptr;
    }
    initializedChunks = 0;
}


std::string DynamicIntArray::toString() {
    std::string result = "{";
    for (int i = 0; i < length - 1; ++i) {
        result += std::to_string(get(i)) + ",";
    }
    if (length > 0) {
        result += std::to_string(get(length - 1));
    }
    result += "}";
    return result;
}


void DynamicIntArray::grow(int chunkIndex) {
    if (chunkIndex >= initializedChunks) {
        if (chunkIndex >= INIT_CHUNKS) {
            int newSize = std::max(chunkIndex + 1, 2 * INIT_CHUNKS);
            int** newChunk = new int*[newSize];
            std::memcpy(newChunk, data, sizeof(int*) * INIT_CHUNKS);
            delete[] data;
            data = newChunk;
        }
        for (int i = initializedChunks; i <= chunkIndex; ++i) {
            data[i] = new int[chunkSize];
        }
        initializedChunks = chunkIndex + 1;
    }
}


int* DynamicIntArray::toArray() {
    if (initializedChunks == 1) {
        return data[0];
    }
    else {
        int* array = new int[length];
        int numChunks = length / chunkSize;
        for (int i = 0; i < numChunks; i++) {
            std::memcpy(array + i * chunkSize, data[i], sizeof(int) * chunkSize);
        }
        int tail = length % chunkSize;
        if (tail > 0) {
            std::memcpy(array + numChunks * chunkSize, data[numChunks], sizeof(int) * tail);
        }
        return array;
    }
}