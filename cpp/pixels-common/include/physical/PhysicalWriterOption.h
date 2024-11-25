//
// Created by gegndy on 24-11-25.
//

#ifndef PIXELS_PHYSICALWRITEROPTION_H
#define PIXELS_PHYSICALWRITEROPTION_H

#include <memory>
#include <cstdint>

class PhysicalWriterOption {
public:
    PhysicalWriterOption(std::int64_t blockSize, bool addBlockPadding, bool overwrite);
    std::int64_t getBlockSize() const;
    std::shared_ptr<PhysicalWriterOption> setBlockSize(std::int64_t blockSize);
    bool isAddBlockPadding() const;
    std::shared_ptr<PhysicalWriterOption> setAddBlockPadding(bool addBlockPadding);
    bool isOverwrite() const;
    std::shared_ptr<PhysicalWriterOption> setOverwrite(bool overwrite);
private:
    std::int64_t blockSize;
    bool addBlockPadding;
    bool overwrite;
};
#endif //PIXELS_PHYSICALWRITEROPTION_H
