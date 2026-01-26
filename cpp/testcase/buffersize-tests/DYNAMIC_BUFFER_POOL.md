# 动态缓冲池 (Dynamic Buffer Pool) 和 io_uring 高级特性

## 概述

本文档介绍基于 io_uring 高级特性实现的可扩容缓冲池 (`DynamicBufferPool`) 和对应的异步文件访问类 (`DirectUringRandomAccessFileDynamic`)。

### 核心特性

- **稀疏注册 (Sparse Registration)**: 使用 `IORING_RSRC_REGISTER_SPARSE` 预注册大量空槽位，无需预先分配实际缓冲区
- **动态分配**: 按需分配缓冲区，节省内存
- **动态更新**: 使用 `IORING_REGISTER_BUFFERS_UPDATE` 在运行时添加/更新/删除缓冲区
- **自动扩容**: 当缓冲区不足时自动扩容
- **高性能**: 利用 io_uring 的零拷贝和批量操作特性

## 架构设计

### DynamicBufferPool

可扩容缓冲池管理器，提供线程本地的缓冲区管理。

**主要组件**:
- `buffer_slots`: 所有缓冲区槽位（最多 1024 个）
- `free_slots`: 空闲槽位队列
- `col_to_slot`: 列 ID 到槽位的映射
- `iovecs`: io_uring 使用的 iovec 数组

**关键方法**:
```cpp
// 初始化缓冲池
static void Initialize(struct io_uring* ring, uint32_t max_slots = 1024);

// 分配缓冲区
static std::shared_ptr<ByteBuffer> AllocateBuffer(uint32_t colId, uint64_t size);

// 获取缓冲区
static std::shared_ptr<ByteBuffer> GetBuffer(uint32_t colId);

// 扩容缓冲区
static std::shared_ptr<ByteBuffer> GrowBuffer(uint32_t colId, uint64_t new_size);

// 释放缓冲区
static void ReleaseBuffer(uint32_t colId);

// 重置缓冲池
static void Reset();
```

### DirectUringRandomAccessFileDynamic

基于动态缓冲池的异步文件访问类。

**主要特性**:
- 自动管理 io_uring 实例
- 支持批量异步读取
- 自动处理 Direct I/O 对齐
- 缓冲区不足时自动扩容

**关键方法**:
```cpp
// 初始化
static void Initialize(uint32_t queue_depth = 4096, uint32_t max_buffer_slots = 1024);

// 异步读取（使用列 ID）
std::shared_ptr<ByteBuffer> readAsync(int length, uint32_t colId);

// 提交所有等待的操作
void readAsyncSubmit(int count);

// 等待操作完成
void readAsyncComplete(int count);

// 提交并等待
void readAsyncExecute(int count);

// 重置
static void Reset();
```

## 使用方法

### 基本用法

```cpp
#include "physical/DynamicBufferPool.h"
#include "physical/natives/DirectUringRandomAccessFileDynamic.h"

// 1. 初始化
DirectUringRandomAccessFileDynamic::Initialize(4096, 1024);

// 2. 分配缓冲区
DynamicBufferPool::AllocateBuffer(0, 64 * 1024);  // 列 0: 64KB
DynamicBufferPool::AllocateBuffer(1, 128 * 1024); // 列 1: 128KB

// 3. 打开文件并读取
DirectUringRandomAccessFileDynamic reader("/path/to/file");
reader.seek(0);
auto data = reader.readAsync(1024, 0);  // 使用列 0 的缓冲区
reader.readAsyncExecute(1);

// 4. 处理数据
std::cout << "Read " << data->size() << " bytes" << std::endl;

// 5. 清理
DirectUringRandomAccessFileDynamic::Reset();
```

### 批量异步读取

```cpp
DirectUringRandomAccessFileDynamic reader("/path/to/file");

// 队列多个读操作
reader.seek(0);
auto data1 = reader.readAsync(1024, 0);

reader.seek(4096);
auto data2 = reader.readAsync(2048, 1);

reader.seek(8192);
auto data3 = reader.readAsync(512, 2);

// 一次性提交并等待所有操作完成
reader.readAsyncExecute(3);

// 现在可以访问所有数据
process(data1);
process(data2);
process(data3);
```

### 动态扩容

```cpp
// 分配小缓冲区
DynamicBufferPool::AllocateBuffer(0, 4096);

// 尝试读取更大的数据 - 自动扩容
DirectUringRandomAccessFileDynamic reader("/path/to/file");
auto data = reader.readAsync(16384, 0);  // 16KB > 4KB
reader.readAsyncExecute(1);

// 或者手动扩容
auto newBuffer = DynamicBufferPool::GrowBuffer(0, 32 * 1024);
```

### 缓冲区管理

```cpp
// 检查缓冲池状态
std::cout << "Total buffers: " << DynamicBufferPool::GetBufferCount() << std::endl;
std::cout << "Max slots: " << DynamicBufferPool::GetMaxSlots() << std::endl;

// 获取缓冲区槽位索引
int slotIdx = DynamicBufferPool::GetBufferSlotIndex(0);

// 释放不需要的缓冲区
DynamicBufferPool::ReleaseBuffer(1);

// 重用释放的槽位
DynamicBufferPool::AllocateBuffer(10, 64 * 1024);
```

## 与传统实现的对比

### 传统 BufferPool (双缓冲)

```cpp
// 固定大小，初始化时全部分配
BufferPool::Initialize(colIds, bytes, columnNames);

// 切换缓冲区
BufferPool::Switch();

// 无法动态调整大小
```

**限制**:
- ❌ 固定缓冲区数量
- ❌ 初始化时必须知道所有列的大小
- ❌ 无法动态扩容
- ❌ 内存利用率低（预分配）

### 新的 DynamicBufferPool

```cpp
// 稀疏注册 - 不需要预分配
DynamicBufferPool::Initialize(ring, 1024);

// 按需分配
DynamicBufferPool::AllocateBuffer(colId, size);

// 动态扩容
DynamicBufferPool::GrowBuffer(colId, new_size);

// 释放和重用
DynamicBufferPool::ReleaseBuffer(colId);
```

**优势**:
- ✅ 动态分配缓冲区
- ✅ 支持运行时扩容
- ✅ 更高的内存利用率
- ✅ 灵活的槽位管理
- ✅ 支持热添加/删除缓冲区

## 性能优化建议

### 1. 合理设置槽位数量

```cpp
// 根据实际需求设置最大槽位
// 大表（100+ 列）
DirectUringRandomAccessFileDynamic::Initialize(4096, 2048);

// 小表（< 50 列）
DirectUringRandomAccessFileDynamic::Initialize(4096, 512);
```

### 2. 预估初始缓冲区大小

```cpp
// 避免频繁扩容，预留一些空间
uint64_t estimatedSize = columnMetadata.avgChunkSize * 1.2;
DynamicBufferPool::AllocateBuffer(colId, estimatedSize);
```

### 3. 批量操作

```cpp
// 好的做法：批量提交
for (int i = 0; i < numReads; i++) {
    reader.readAsync(size, colIds[i]);
}
reader.readAsyncExecute(numReads);  // 一次性提交

// 避免：逐个操作
for (int i = 0; i < numReads; i++) {
    auto data = reader.readAsync(size, colIds[i]);
    reader.readAsyncExecute(1);  // 效率低
}
```

### 4. 及时释放不需要的缓冲区

```cpp
// 处理完某列后释放缓冲区
processColumn(colId);
DynamicBufferPool::ReleaseBuffer(colId);
```

## io_uring 特性说明

### IORING_RSRC_REGISTER_SPARSE

允许注册大量槽位而不实际分配缓冲区：

```cpp
// 注册 1024 个空槽位
struct iovec iovecs[1024] = {0};
io_uring_register_buffers(ring, iovecs, 1024);
```

### IORING_REGISTER_BUFFERS_UPDATE

动态更新已注册的缓冲区：

```cpp
// 在槽位 5 注册新缓冲区
struct iovec new_iov;
new_iov.iov_base = buffer->getPointer();
new_iov.iov_len = buffer->size();
io_uring_register_buffers_update(ring, 5, &new_iov, 1);
```

### Fixed Buffer Read

使用已注册的缓冲区进行零拷贝读取：

```cpp
io_uring_prep_read_fixed(sqe, fd, buffer, length, offset, buf_index);
```

## 示例程序

### 运行测试

```bash
# 编译测试程序
cd /home/whz/test/pixels/cpp
make tests/dynamic_buffer_pool_test

# 运行测试
./tests/dynamic_buffer_pool_test
```

### 运行示例

```bash
# 编译示例
make examples/dynamic_buffer_example

# 运行示例
./examples/dynamic_buffer_example /path/to/your/file.pxl
```

## 错误处理

### 常见错误

1. **槽位不足**
```cpp
try {
    DynamicBufferPool::AllocateBuffer(colId, size);
} catch (const InvalidArgumentException& e) {
    // 增加 max_buffer_slots 或释放一些缓冲区
}
```

2. **缓冲区不存在**
```cpp
auto buffer = DynamicBufferPool::GetBuffer(colId);
if (buffer == nullptr) {
    // 先分配缓冲区
    buffer = DynamicBufferPool::AllocateBuffer(colId, size);
}
```

3. **io_uring 初始化失败**
```cpp
try {
    DirectUringRandomAccessFileDynamic::Initialize();
} catch (const InvalidArgumentException& e) {
    // 检查系统是否支持 io_uring
    // 检查 ulimit -n（文件描述符限制）
}
```

## 线程安全

⚠️ **重要**: `DynamicBufferPool` 和 `DirectUringRandomAccessFileDynamic` 使用线程本地存储，每个线程有独立的缓冲池实例。

```cpp
// 线程 A
DirectUringRandomAccessFileDynamic::Initialize();
DynamicBufferPool::AllocateBuffer(0, 1024);

// 线程 B - 需要独立初始化
DirectUringRandomAccessFileDynamic::Initialize();
DynamicBufferPool::AllocateBuffer(0, 1024);  // 不同的缓冲池
```

## 系统要求

- Linux 内核 >= 5.11（支持 io_uring buffer update）
- liburing >= 2.0
- C++11 或更高版本
- 足够的内存锁定限制（ulimit -l）

## 性能测试结果

（待完成基准测试后更新）

## 参考资料

- [io_uring Documentation](https://kernel.dk/io_uring.pdf)
- [liburing API](https://github.com/axboe/liburing)
- [Efficient IO with io_uring](https://kernel.dk/io_uring.pdf)

## 作者

- whz (@whzruc)
- 创建日期: 2026-01-23

## 许可证

Affero GNU General Public License v3.0
