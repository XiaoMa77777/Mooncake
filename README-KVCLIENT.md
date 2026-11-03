# Mooncake 原生 offload 接入 kvclient

本适配器挂接 Mooncake Store 的 `StorageBackendInterface`：原生 Client 的内存缓存进入既有 offload 流程后，经 `KvclientStorageBackend → aistor::KvclientStorageBackend → kvclient SDK` 写入阵列；回读、元数据恢复继续由 Mooncake 原生流程管理。业务继续使用原生 Client API，但需要安装启用了该后端的新 Mooncake 客户端库或 Python wheel。只给原版 wheel 增加环境变量不会获得此能力。

## 构建与安装

使用 Linux、已具备 RDMA 依赖的 Mooncake 构建环境。先安装带 `kvclient::mooncake` CMake target 的 SDK；SDK 的 Mooncake 适配库必须使用当前 Mooncake 源码的接口头构建，避免混用不同版本的 C++ 接口。以下 `/opt/kvclient` 为 SDK 安装前缀示例。

```bash
cmake -S . -B build-kvclient \
  -DWITH_KVCLIENT_BACKEND=ON \
  -DCMAKE_PREFIX_PATH=/opt/kvclient \
  -DBUILD_UNIT_TESTS=ON \
  -DWITH_STORE_RUST=OFF
cmake --build build-kvclient -j8
cmake --install build-kvclient --prefix /opt/mooncake-kvclient
```

`WITH_KVCLIENT_BACKEND` 默认 `OFF`；关闭时选择 `kvclient_storage_backend` 会明确报不支持，不会自动落回本地文件后端。若构建缺少 `kvclient::mooncake`，配置阶段直接失败。

Python 使用方需重新构建并安装本源码的 wheel；普通 CMake 安装不会替换已有 Python 包：

```bash
python -m pip wheel . --no-deps -w dist-kvclient \
  -Ccmake.define.WITH_KVCLIENT_BACKEND=ON \
  -Ccmake.define.CMAKE_PREFIX_PATH=/opt/kvclient
python -m pip install --force-reinstall --no-deps /path/to/new-mooncake.whl
```

按实际生成的文件名替换安装命令中的 wheel 路径。部署前应在目标环境确认新包的动态库依赖可以加载；SDK 共享库必须位于系统库目录或进程的 `LD_LIBRARY_PATH` 中。

## 运行配置与恢复

先创建持久目录，再将这些变量设置到承载 offload 的 Mooncake Client 进程中；Master 仍需按原生部署开启 `--enable_offload=true`，客户端仍需启用已有的磁盘 offload 初始化流程。

```bash
mkdir -p /var/lib/mooncake/store-a
export MOONCAKE_OFFLOAD_STORAGE_BACKEND_DESCRIPTOR=kvclient_storage_backend
export MOONCAKE_OFFLOAD_FILE_STORAGE_PATH=/var/lib/mooncake/store-a
export MOONCAKE_OFFLOAD_KVCLIENT_CONFIG_PATH=/etc/kvclient/client.yaml
export MOONCAKE_OFFLOAD_KVCLIENT_INDEX_PATH=/var/lib/mooncake/store-a/client.index
export MOONCAKE_OFFLOAD_KVCLIENT_PREFIX=mooncake-offload
export MOONCAKE_OFFLOAD_KVCLIENT_LAYOUT=v1
export MOONCAKE_OFFLOAD_KVCLIENT_DEVICE_PAYLOADS=false
```

`store-a` 是部署方为这个客户端分配的稳定身份。重启后必须沿用同一个索引文件、格式配置和阵列命名空间；索引文件名不能使用 PID。并发客户端各用独立的稳定索引路径，同一路径的第二个持有者会因文件锁而初始化失败。不要删除索引文件后宣称恢复成功：`ScanMeta` 只恢复本索引登记的对象，不扫描整个阵列。

索引路径优先级是 `MOONCAKE_OFFLOAD_KVCLIENT_INDEX_PATH`（必须是绝对文件路径），其次 `MOONCAKE_OFFLOAD_KVCLIENT_INDEX_DIR/client.index`，最后 `MOONCAKE_OFFLOAD_FILE_STORAGE_PATH/client.index`。即使设置了独立 `INDEX_PATH`，原生 FileStorage 配置仍要求 `FILE_STORAGE_PATH` 是已存在的绝对目录；显式索引路径的父目录也应预先创建。`INDEX_DIR` 中历史的 `client-<pid>.index` 不会自动迁移，可在旧进程停止后用 `INDEX_PATH` 显式指向需要恢复的那一份索引。

可选参数 `MOONCAKE_OFFLOAD_KVCLIENT_MAX_OBJECT_BYTES` 默认 4194304（4 MiB），范围为 1 至 4194304；`MOONCAKE_OFFLOAD_KVCLIENT_TIMEOUT_MS` 默认 15000。旧的 `MOONCAKE_OFFLOAD_KVCLIENT_FRAGMENT_BYTES` 已随单值格式移除，仍设置它会在启动时明确报错。读取既有对象时应保持 `PREFIX` 和 `LAYOUT` 一致。

当前封装支持 **HOST Value**。`DEVICE_PAYLOADS=true` 会被拒绝，因为原生封装尚未注册设备 arena；不能据此宣称支持显存直达。每个逻辑 key 对应一个物理 Value，上限为 4 MiB 且可经 `MAX_OBJECT_BYTES` 调小；不再有远端 manifest、分片或跨对象组合发布。`ScanMeta` 一次调用会完成所有分批回调；回调错误会原样返回，失败批次可重试，`ResetScanIterator` 从头重新扫描。

## 验证

无需阵列的配置校验始终运行。阵列测试仅在未设置 SDK 配置路径时跳过；配置已设置但连接或操作失败会判失败。实机验收须强制要求阵列配置：

```bash
export MOONCAKE_OFFLOAD_KVCLIENT_CONFIG_PATH=/etc/kvclient/client.yaml
export MOONCAKE_KVCLIENT_TEST_REQUIRE_ARRAY=1
ctest --test-dir build-kvclient -R '^kvclient_backend_test$' --output-on-failure
```

该测试覆盖 4 MiB + 257 B 多 Slice 数据逐字节回读、缺失对象、工厂配置重开、索引锁互斥、扫描回调失败重试，以及不同 PID 间 300 个对象的完整恢复和内容校验。跨进程测试分别启动全新 writer/reader；每个子进程限时 180 秒，整套 CTest 限时 600 秒。测试使用独立前缀和索引目录；跨进程失败时保留索引目录供检查。这个后端测试不能代替原生 Store 的 offload/恢复端到端验收，也不代表设备路径或全部故障场景已经通过。

默认不执行的 `DISABLED_RealisticThroughput` 用于短时吞吐测量：每批 128 个 1.25 MiB HOST 对象，共三批，每批先写后读，并逐字节校验全部内容。使用 fixture 的独立前缀和索引，结束后清理本轮对象；总计写入和读出各 480 MiB。需要手动对构建出的 `kvclient_backend_test` 可执行文件传入：

```bash
--gtest_also_run_disabled_tests \
--gtest_filter=KvclientBackendTest.DISABLED_RealisticThroughput
```

标准输出的 `KVCLIENT_BENCH` 行给出每批和汇总的 PUT/GET `objs_per_sec`、`mib_per_sec` 及耗时；计时包括后端调用，排除数据准备、内容校验和最终清理。该测量是单个后端实例的顺序批调用，不模拟原生 Store 的并发调度，也不代表持续压测结果。
