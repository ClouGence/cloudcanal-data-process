## 简介 
自定义代码允许用户将带有数据处理逻辑或业务逻辑的代码上传到 CloudCanal, CloudCanal 的全量迁移和增量同步任务运行时自动调用这些代码以达到多种数据变换、处理的目的。

## 子工程介绍
- **wide-table**
  - 打宽表数据处理插件，主要包括事实表和单维表组合处理
- **data-transform**
  - 数据通用转换插件，比如做操作变幻、额外加字段、清洗回填数据
- **data-gather**
  - 数据汇聚插件，将分库分表、垂直拆分、异地数据进行实时汇聚
- **data-compare**
  - 数据对比插件，根据源端数据变化进行业务对账
- **business-alert**
  - 业务告警插件，根据数据变化趋势做出相应告警

## 使用步骤
- [安装并激活 CloudCanal](https://www.clougence.com/cc-doc/productOP/systemDeploy/install_linux_macos)
- [创建自定义代码任务](https://www.clougence.com/cc-doc/operation/job_manage/create_job/create_process_job)
- 任务正常运行

## 其他文档
- [Debug 自定义代码](https://www.clougence.com/cc-doc/operation/job_manage/convenience_features/debug_customer_code)
- [在自定义代码中打日志](https://www.clougence.com/cc-doc/operation/job_manage/convenience_features/log_in_customer_code)

## README.md
[English](../README.md)
[简体中文](README.zh_CN.md)